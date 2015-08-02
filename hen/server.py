from dispatcher import Dispatcher
from time import sleep

if __name__ == "__main__":

    disp = Dispatcher(9001)

    try:
        while True:
            for fd, msg in disp.poll():
                if msg == "CONNECTED":
                    disp.message(fd, "Howdy!\n")
                elif msg == "DISCONNECTED":
                    pass
                else:
                    disp.message(fd, "Lolwut?\n")
            sleep(0.05)
    except KeyboardInterrupt:
        pass

    disp.kill()
    print("Bye!")
