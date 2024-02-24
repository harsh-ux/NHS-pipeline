import subprocess
import socket
import threading
import time
import os
import signal

def run_simulator(simulator_script):
    """
    Function to run the simulator script in a separate thread.
    This allows the simulator to send requests independently of the main script.
    """
    subprocess.run(['python3', simulator_script])

def start_main_process(main_script):
    """
    Function to start the main process and return the process object.
    """
    return subprocess.Popen(['python3', main_script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def simulate_requests_to_main(host, port, num_requests=5, delay=2):
    """
    Function to simulate requests to the main.py script listening on a socket.
    Encodes messages in MLLP format.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        for _ in range(num_requests):
            # Simulate an MLLP-encoded request
            mllp_message = b'\x0b' + b'Test request' + b'\x1c\x0d'
            s.sendall(mllp_message)
            time.sleep(delay)

def run_integration_test(simulator_script, main_script, host="127.0.0.1", port=8440):
    """
    Main function to run the integration test.
    """
    # Start the simulator in a separate thread to mimic its behavior
    simulator_thread = threading.Thread(target=run_simulator, args=(simulator_script,))
    simulator_thread.start()

    # Start the main process
    main_process = start_main_process(main_script)

    # Give some time for the main script to initialize and start listening
    time.sleep(5)

    # Simulate requests to the main script as if they were coming from the simulator
    simulate_requests_to_main(host, port)

    # Wait for the main process to complete its execution
    try:
        stdout, stderr = main_process.communicate(timeout=30)  # Adjust timeout as necessary
    except subprocess.TimeoutExpired:
        print("Main process did not finish within the timeout period.")
        os.kill(main_process.pid, signal.SIGINT)  # Attempt graceful shutdown
        simulator_thread.join()  # Ensure simulator thread is cleaned up
        return

    print("Main process completed. Output:", stdout.decode())

    # Cleanup
    simulator_thread.join()  # Wait for the simulator thread to finish

    print("Integration test completed successfully.")

if __name__ == "__main__":
    SIMULATOR_SCRIPT = 'simulator.py'
    MAIN_SCRIPT = 'main.py'
    run_integration_test(SIMULATOR_SCRIPT, MAIN_SCRIPT)
