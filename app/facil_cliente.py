# file: lpd_client.py
import socket

class LPDClient:
    def __init__(self, host: str, port: int = 515):
        """
        Initialize the LPD client.
        :param host: Address of the print server.
        :param port: Port for LPD communication (default: 515).
        """
        self.host = host
        self.port = port

    def _send_command(self, command: str):
        """
        Send a command to the LPD server.
        :param command: Command to send.
        :return: Response from the server.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            s.sendall(command.encode('utf-8') + b'\n')
            response = s.recv(1024)
        return response

    def print_job(self, queue_name: str, file_path: str):
        """
        Submit a print job to the LPD server.
        :param queue_name: Print queue name on the server.
        :param file_path: Path to the file to print.
        """
        # Step 1: Send the "Receive a print job" command
        command = f"\x02{queue_name}\n"
        print("Sending print job command...")
        response = self._send_command(command)
        if response != b'\x00':
            raise Exception("Failed to initiate print job")

        # Step 2: Send the control file
        control_data = f"H{self.host}\nPuser\nfdata\n"
        control_file_size = len(control_data)
        command = f"\x02{control_file_size} cfA001{self.host}\n"
        self._send_command(command)
        self._send_command(control_data)

        # Step 3: Send the data file
        with open(file_path, 'rb') as f:
            data = f.read()
        data_file_size = len(data)
        command = f"\x03{data_file_size} dfA001{self.host}\n"
        self._send_command(command)
        self._send_command(data)

        # Finish the print job
        print("Print job sent successfully!")


# Example usage:
if __name__ == "__main__":
    client = LPDClient(host="192.168.1.100")  # Replace with your print server IP
    client.print_job(queue_name="default", file_path="test.txt")


