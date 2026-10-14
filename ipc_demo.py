import multiprocessing
import socket
import json
import time
import os

HOST = "127.0.0.1"
PORT = 50505
QUEUE_FILE = "offline_queue.jsonl"
RECEIVED_FILE = "server_received.jsonl"


def save_json_line(filename, data):
    with open(filename, "a", encoding="utf-8") as file:
        file.write(json.dumps(data, ensure_ascii=False) + "\n")


def run_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()

    print(f"[SERVER] Menunggu koneksi di {HOST}:{PORT}")

    try:
        while True:
            connection, address = server.accept()
            with connection:
                print(f"[SERVER] Terhubung dengan {address}")

                with connection.makefile("r", encoding="utf-8") as reader:
                    for line in reader:
                        try:
                            data = json.loads(line)
                            save_json_line(RECEIVED_FILE, data)
                            print("[SERVER] Pesan diterima:", data)
                            connection.sendall(b"ACK\n")
                        except json.JSONDecodeError:
                            connection.sendall(b"ERROR\n")
    except KeyboardInterrupt:
        print("\n[SERVER] Server dihentikan.")
    finally:
        server.close()


def send_pending_messages():
    if not os.path.exists(QUEUE_FILE):
        return

    try:
        with open(QUEUE_FILE, "r", encoding="utf-8") as file:
            messages = [line for line in file if line.strip()]

        if not messages:
            return

        with socket.create_connection((HOST, PORT), timeout=3) as connection:
            reader = connection.makefile("r", encoding="utf-8")
            sent = 0

            for line in messages:
                connection.sendall(line.encode("utf-8"))
                if reader.readline().strip() == "ACK":
                    sent += 1
                else:
                    break

        remaining = messages[sent:]
        with open(QUEUE_FILE, "w", encoding="utf-8") as file:
            file.writelines(remaining)

        print(f"[WORKER] Pesan berhasil dikirim: {sent}")

    except (OSError, ConnectionError) as error:
        print("[WORKER] Server belum tersedia. Pesan disimpan offline.")
        print("[WORKER] Detail:", error)


def worker(pipe):
    while True:
        message = pipe.recv()

        if message == "STOP":
            break

        save_json_line(QUEUE_FILE, message)
        print("[WORKER] Pesan disimpan ke antrean offline.")
        send_pending_messages()

    pipe.close()


def run_client():
    parent_pipe, child_pipe = multiprocessing.Pipe()
    process = multiprocessing.Process(target=worker, args=(child_pipe,))
    process.start()
    child_pipe.close()

    print("Ketik pesan, lalu Enter. Ketik 'keluar' untuk berhenti.")

    try:
        while True:
            text = input("Pesan: ")

            if text.lower() == "keluar":
                break

            message = {
                "pesan": text,
                "waktu": time.strftime("%Y-%m-%d %H:%M:%S"),
                "pengirim": socket.gethostname()
            }

            parent_pipe.send(message)
            print("[CLIENT] Pesan dikirim ke worker melalui Pipe.")

    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        parent_pipe.send("STOP")
        parent_pipe.close()
        process.join()
        print("[CLIENT] Program selesai.")


if __name__ == "__main__":
    multiprocessing.freeze_support()

    import sys
    if len(sys.argv) != 2 or sys.argv[1] not in ("server", "client"):
        print("Cara menjalankan:")
        print("  py ipc_demo.py server")
        print("  py ipc_demo.py client")
    elif sys.argv[1] == "server":
        run_server()
    else:
        run_client()