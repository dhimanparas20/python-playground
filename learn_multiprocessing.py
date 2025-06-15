import multiprocessing
from time import sleep, perf_counter
from multiprocessing import Process, current_process, active_children
from os import system

system("clear")

def counter_function(name, lmt):
    print("---------------------------------------")
    print(f"Process {name} (PID: {current_process().pid}) is Counting till: {lmt}")
    for i in range(lmt):
        print(f"{name} : {i=}, PID: {current_process().pid}")
        sleep(0.1)

if __name__ == "__main__":
    start = perf_counter()
    counter_function("A", 50)
    counter_function("B", 25)
    counter_function("C", 30)
    counter_function("D", 10)
    print(f"Time taken (sequential): {perf_counter() - start:.2f} seconds\n")

    # Create processes
    p1 = Process(target=counter_function, args=("a", 50))
    p2 = Process(target=counter_function, args=("b", 25))
    p3 = Process(target=counter_function, args=("c", 30))
    p4 = Process(target=counter_function, args=("d", 10))

    processes = [p1, p2, p3, p4]

    start = perf_counter()
    for p in processes:
        p.start()

    print(f"\nActive processes (not including main): {len(active_children())}")
    for proc in active_children():
        print("-------------------------------------")
        print("Process Name:", proc.name)
        print("Is alive: ", proc.is_alive())
        print("PID: ", proc.pid)
        print("Compare PID: ", proc.pid == current_process().pid)
        print("IDF wtf:", proc.pid is current_process().pid)

    for p in processes:
        p.join()

    print(f"Time taken (parallel): {perf_counter() - start:.2f} seconds\n")

    print(f"Total processes (including main): {len(multiprocessing.active_children()) + 1}")
    print("Main process PID:", current_process().pid)
