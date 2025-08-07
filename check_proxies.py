import threading
import queue
import requests

q = queue.Queue()
valid_proxies = []

# Abrimos el archivo y extraemos solo la columna IP
with open("data/proxies/Free_Proxy_List.txt", "r") as f:
    proxies = f.read().split("\n")
    for p in proxies:
        q.put(p)


def check_proxies():
    while not q.empty():
        proxy = q.get()
        try:
            res = requests.get("http://ipinfo.io/json",
                               proxies={"http": proxy, "https": proxy},
                               timeout=5)
            if res.status_code == 200:
                print(f"{proxy}")
                valid_proxies.append(proxy)
        except:
            pass  # Silenciamos errores de conexión


# Lanzamos 10 hilos
for _ in range(50):
    threading.Thread(target=check_proxies).start()
