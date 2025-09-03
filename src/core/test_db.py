from .db import ping

if __name__ == "__main__":
    try:
        ver = ping()
        print("Подключение ОК ✅")
        print(ver)
    except Exception as e:
        print("Ошибка подключения:", e)
        raise
