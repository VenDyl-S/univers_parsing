import time
from datetime import datetime
import itertools
import requests
from rich import print

PROXIES = [

]
RENEW_URL = (""
             "")
IP_CHECK_URL = "https://api.ipify.org"          # Возвращает IP простым текстом
POLL_INTERVAL = 3                               # секунд
NUM_CYCLES = 3                                  # сколько раз измерить смену

def build_proxy_dict(raw: str):
    host, port, user, pwd = raw.split(":")
    proxy_auth = f"http://{user}:{pwd}@{host}:{port}"
    return {"http": proxy_auth, "https": proxy_auth}

def get_external_ip(proxies: dict, timeout: int = 10) -> str | None:
    try:
        r = requests.get(IP_CHECK_URL, proxies=proxies, timeout=timeout)
        r.raise_for_status()
        return r.text.strip()
    except requests.RequestException as e:
        print(f"[red]Ошибка запроса IP: {e}[/red]")
        return None

def renew_proxy_ip():
    try:
        r = requests.get(RENEW_URL, timeout=15)
        r.raise_for_status()
        print(f"[cyan]{datetime.now():%H:%M:%S} → Запрос на смену IP отправлен.[/cyan]")
        print(f"[dim]Ответ API: {r.text.strip()}[/dim]")
    except requests.RequestException as e:
        print(f"[red]Ошибка запроса renew: {e}[/red]")

def measure_cycle(proxy_raw: str, cycle_no: int):
    print(f"\n[bold green]=== Цикл {cycle_no} | Прокси: {proxy_raw} ===[/bold green]")
    proxies = build_proxy_dict(proxy_raw)

    # 1) Текущий IP
    old_ip = get_external_ip(proxies)
    if old_ip is None:
        return
    print(f"[yellow]{datetime.now():%H:%M:%S} → Текущий IP: {old_ip}[/yellow]")

    # 2) Рестарт IP
    renew_proxy_ip()
    t0 = time.perf_counter()

    # 3) Поллинг до смены
    while True:
        time.sleep(POLL_INTERVAL)
        new_ip = get_external_ip(proxies)
        if new_ip and new_ip != old_ip:
            dt = time.perf_counter() - t0
            print(f"[green]{datetime.now():%H:%M:%S} → IP изменился: {old_ip} → {new_ip} "
                  f"за {dt:.1f} с[/green]")
            break
        else:
            print(f"[dim]{datetime.now():%H:%M:%S} … IP пока тот же[/dim]")

def main():
    proxy_cycle = itertools.cycle(PROXIES)  # чередуем несколько адресов
    for i in range(1, NUM_CYCLES + 1):
        measure_cycle(next(proxy_cycle), i)

if __name__ == "__main__":
    main()