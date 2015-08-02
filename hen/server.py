from dispatcher import Dispatcher
from time import sleep

if __name__ != "__main__":
    print("Well, that's funny. We aren't main. Not gonna run.")
else:
    d = Dispatcher(9001)

    while True:
        try:
            d.poll()
            sleep(0.05)
        except KeyboardInterrupt:
            break
    d.kill()
    print("Bye!")
