# file: lpd_service.py
import os
import socket
import logging
from concurrent.futures import ThreadPoolExecutor
import win32serviceutil
import win32service
import win32event
import servicemanager

# Configure logging
LOG_FILE = "./lpd_server.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class LPDServer:
    def __init__(self, host="0.0.0.0", port=515, spool_dir="./spool", max_threads=10):
        """
        Initialize the LPD server.
        :param host: Host to bind the server (default: 0.0.0.0).
        :param port: Port for the LPD protocol (default: 515).
        :param spool_dir: Directory to store print jobs.
        :param max_threads: Maximum number of threads in the thread pool.
        """
        self.host = host
        self.port = port
        self.spool_dir = spool_dir
        self.max_threads = max_threads
        os.makedirs(spool_dir, exist_ok=True)
        self.server_socket = None
        self.running = False

    def receive_large_file(self, conn, file_path, file_size, chunk_size=4096, retries=3):
        """
        Receive a large file from a connection and save it to disk with retries.
        :param conn: The client socket.
        :param file_path: Path to save the received file.
        :param file_size: Total size of the file in bytes.
        :param chunk_size: Size of each chunk to receive (default: 4 KB).
        :param retries: Number of retries if connection fails.
        """
        received_size = 0

        for attempt in range(retries):
            try:
                with open(file_path, "ab") as file:
                    if attempt > 0:
                        logging.warning(f"Retrying to receive file: {file_path} (Attempt {attempt + 1})")
                    while received_size < file_size:
                        remaining = file_size - received_size
                        chunk = conn.recv(min(chunk_size, remaining))
                        if not chunk:
                            raise ConnectionError("Connection closed prematurely")
                        file.write(chunk)
                        received_size += len(chunk)
                        logging.info(f"Received {received_size}/{file_size} bytes for file {file_path}")
                break
            except Exception as e:
                logging.error(f"Error receiving file {file_path}: {e}")
                if attempt == retries - 1:
                    raise Exception(f"Failed to receive file after {retries} attempts")
        logging.info(f"File {file_path} received successfully (Size: {file_size} bytes)")

    def handle_connection(self, conn, addr):
        """
        Handle an incoming connection from a client.
        """
        logging.info(f"Connection established with {addr}")
        try:
            while True:
                command = conn.recv(1)
                if not command:
                    break

                if command == b'\x02':  # Receive a print job
                    self.receive_print_job(conn)
                else:
                    logging.warning(f"Unknown command received: {command}")
                    conn.sendall(b'\x01')  # Send an error code
        except Exception as e:
            logging.error(f"Error handling connection from {addr}: {e}")
        finally:
            conn.close()
            logging.info(f"Connection closed with {addr}")

    def receive_print_job(self, conn):
        """
        Handle the 'Receive Print Job' command.
        """
        queue_name = conn.recv(1024).decode('utf-8').strip()
        logging.info(f"Receiving print job for queue: {queue_name}")
        conn.sendall(b'\x00')

        control_file_size = self._read_size_prefixed_data(conn, "control file")
        control_file_path = os.path.join(self.spool_dir, f"cf_{queue_name}.txt")
        control_file_data = conn.recv(control_file_size).decode('utf-8')
        with open(control_file_path, "w") as f:
            f.write(control_file_data)
        conn.sendall(b'\x00')
        logging.info(f"Control file saved to {control_file_path}")

        data_file_size = self._read_size_prefixed_data(conn, "data file")
        data_file_path = os.path.join(self.spool_dir, f"df_{queue_name}.txt")
        self.receive_large_file(conn, data_file_path, data_file_size)
        conn.sendall(b'\x00')

        logging.info(f"Print job received successfully for queue: {queue_name}")

    def _read_size_prefixed_data(self, conn, file_type):
        """
        Read a size-prefixed command for file transfer.
        """
        size_data = conn.recv(1024).decode('utf-8').strip()
        logging.info(f"Receiving {file_type} with size: {size_data}")
        return int(size_data.split()[0])

    def start(self):
        """
        Start the LPD server using a thread pool for multithreading.
        """
        logging.info(f"Starting LPD server on {self.host}:{self.port}...")
        self.running = True
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen(5)
            self.server_socket = server_socket
            logging.info("LPD server is running and waiting for connections...")

            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                while self.running:
                    try:
                        conn, addr = server_socket.accept()
                        executor.submit(self.handle_connection, conn, addr)
                    except Exception as e:
                        logging.error(f"Error accepting connection: {e}")

    def stop(self):
        """
        Stop the LPD server.
        """
        logging.info("Stopping LPD server...")
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        logging.info("LPD server stopped.")


class LPDService(win32serviceutil.ServiceFramework):
    _svc_name_ = "LPDService"
    _svc_display_name_ = "LPD Server Service"
    _svc_description_ = "A Line Printer Daemon (LPD) server implemented in Python."

    def __init__(self, args):
        super().__init__(args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.server = LPDServer()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.server.stop()
        win32event.SetEvent(self.stop_event)

    def SvcDoRun(self):
        servicemanager.LogInfoMsg("Starting LPD Server Service...")
        self.server.start()


if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(LPDService)

