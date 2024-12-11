import random
from collections import deque
import time

class Process:
    def __init__(self, name, burst_time, io_operations=0):
        self.name = name               # 进程名称
        self.burst_time = burst_time   # 总的执行时间（CPU 执行时间）
        self.remaining_time = burst_time # 剩余执行时间
        self.io_operations = io_operations  # I/O 操作次数
        self.executed_time = 0         # 已执行的 CPU 时间
        self.priority = 0              # 当前优先级队列（0 为最高，依次递减）

    def __str__(self):
        return f"{self.name} (Priority {self.priority}) - Remaining Time: {self.remaining_time}"

class MLFQ:
    def __init__(self, time_quantum, max_priority=3):
        self.time_quantum = time_quantum  # 每个队列的时间片（时间配额）
        self.max_priority = max_priority  # 最大优先级
        self.queues = {i: deque() for i in range(self.max_priority)}  # 队列存储每个优先级的进程
        self.time = 0                      # 系统的当前时间
        self.processes = []                # 系统中的所有进程

    def add_process(self, process):
        """ 将进程添加到最高优先级队列 """
        self.queues[0].append(process)
        self.processes.append(process)

    def run(self):
        """ 运行 MLFQ 算法，处理所有进程 """
        while any(self.queues.values()):  # 只要有队列中还有进程
            if self.time >= 10:
                print("Time 10s, all processes go back to the highest priority queue")
                self.time = 0
                # 当运行的时间大于10的时候，就把所有进程重新放回最高优先级队列中
                for p in range(1, self.max_priority):
                    while self.queues[p]:
                        proc = self.queues[p].popleft()
                        proc.priority = 0  # 设置进程优先级为最高
                        self.queues[0].append(proc)
                        print(f"Time {self.time}: {proc.name} moved to queue 0")
            for priority in range(self.max_priority):
                if self.queues[priority]:
                    self.time += 1
                    process = self.queues[priority].popleft()  # 从队列中取出一个进程
                    print(f"Time {self.time}: {process}")
                    
                    # 运行该进程
                    self.execute_process(process)
                    
                    # 根据进程的执行结果决定是否降级或保持优先级
                    if process.remaining_time > 0:
                        if process.io_operations > 0:
                            # 如果进程进行 I/O 操作，保持在当前队列
                            process.priority = priority
                            process.io_operations -= 1
                            self.queues[process.priority].append(process)
                            print(f"{process.name} performed I/O and stays at priority {process.priority}")
                            print(f"{process.name} remaining time after execution: {process.remaining_time}")
                            break
                        # 如果还需要更多时间，降级到下一个队列
                        if priority < self.max_priority - 1:
                            process.priority = priority + 1
                            self.queues[process.priority].append(process)
                            print(f"{process.name} moved to priority {process.priority}")
                            print(f"{process.name} remaining time after execution: {process.remaining_time}")
                            break
                        if priority >= self.max_priority - 1:
                            # 如果说已经到达最低优先级，则等待
                            process.priority = priority
                            self.queues[process.priority].append(process)
                            print(f"{process.name} waits at priority {process.priority}")
                            print(f"{process.name} remaining time after execution: {process.remaining_time}")
                            break
                    else:
                        # 如果进程已经完成，从队列中移除
                        print(f"{process.name} have finished execution")
                        break

    def execute_process(self, process):
        """ 执行进程，模拟 CPU 时间消耗 """
        if process.remaining_time <= self.time_quantum:
            # 如果进程能在当前时间片内完成
            print(f"{process.name} executed in time quantum can be finished !!!")
            process.executed_time += process.remaining_time
            process.remaining_time = 0
        else:
            # 如果进程需要更多的时间
            process.executed_time += self.time_quantum
            process.remaining_time -= self.time_quantum

        # 随机决定是否发生 I/O 操作（假设 20% 的概率发生 I/O）
        # if random.random() < 0.2:
        #     process.io_operations += 1
        #     print(f"{process.name} performed I/O operation.")

# 测试 MLFQ 算法

def test_mlfq():
    mlfq = MLFQ(time_quantum=5, max_priority=3)
    
    # 创建一些进程
    p1 = Process(name="P1", burst_time=12)
    p2 = Process(name="P2", burst_time=8)
    p3 = Process(name="P3", burst_time=15)
    p4 = Process(name="P4", burst_time=5)
    
    # 添加进程到 MLFQ
    mlfq.add_process(p1)
    mlfq.add_process(p2)
    mlfq.add_process(p3)
    mlfq.add_process(p4)
    
    # 运行调度算法
    print("=== Running MLFQ At First ===")
    mlfq.run()


# 测试1
# 基本调度测试（不同时间片大小）
# 该测试主要是验证MLFQ算法的基础功能，包括进程的优先级调整和调度
def test_basic_scenario():
    mlfq = MLFQ(time_quantum=5, max_priority=3)
    
    # 创建一些进程
    p1 = Process(name="P1", burst_time=10)
    p2 = Process(name="P2", burst_time=7)
    p3 = Process(name="P3", burst_time=15)
    
    # 添加进程到 MLFQ
    mlfq.add_process(p1)
    mlfq.add_process(p2)
    mlfq.add_process(p3)
    
    # 运行调度算法
    print("=== Basic Scheduling Test ===")
    mlfq.run()

# 测试2
# I/O 操作测试
# 这个测试模拟了进程频繁执行 I/O 操作的情况，看看 I/O 操作对进程优先级调整和调度的影响。
def test_io_operations():
    mlfq = MLFQ(time_quantum=5, max_priority=3)
    
    # 创建进程，其中 P1 具有较高的 I/O 操作频率
    p1 = Process(name="P1", burst_time=30, io_operations=3)
    p2 = Process(name="P2", burst_time=12, io_operations=0)
    
    # 添加进程到 MLFQ
    mlfq.add_process(p1)
    mlfq.add_process(p2)
    
    # 运行调度算法
    print("=== I/O Operations Test ===")
    mlfq.run()


# 测试3
# 长时间CPU占用的进程的测试
# 这个测试模拟了一个长时间占用CPU的进程，看看它是否会被降级，并测试算法如何在资源被占用的情况下调度其他进程
def test_long_cpu_burst():
    mlfq = MLFQ(time_quantum=5, max_priority=3)
    
    # 创建进程，其中 P1 为 CPU 密集型进程
    p1 = Process(name="P1", burst_time=80)
    p2 = Process(name="P2", burst_time=8)
    
    # 添加进程到 MLFQ
    mlfq.add_process(p1)
    mlfq.add_process(p2)
    
    # 运行调度算法
    print("=== Long CPU Burst Test ===")
    mlfq.run()
    

# 测试4
# 进程执行时间较短
# 该测试模拟短时间执行的进程，验证算法能否被正确调度短作业进程，避免饥饿现象
def test_short_jobs():
    mlfq = MLFQ(time_quantum=5, max_priority=3)
    
    # 创建进程，其中 P1 为短时间作业
    p1 = Process(name="P1", burst_time=3)
    p2 = Process(name="P2", burst_time=6)
    
    # 添加进程到 MLFQ
    mlfq.add_process(p1)
    mlfq.add_process(p2)
    
    # 运行调度算法
    print("=== Short Jobs Test ===")
    mlfq.run()

# 测试5
# 该测试模拟了多个进程且每个进程行为不一样，目的在于验算算法能否合理调度多个优先级队列中的进程
def test_multiple_processes():
    mlfq = MLFQ(time_quantum=4, max_priority=3)
    
    # 创建多个进程，混合 CPU 密集型和 I/O 密集型进程
    p1 = Process(name="P1", burst_time=10, io_operations=1)   # I/O 密集型
    p2 = Process(name="P2", burst_time=15)                    # CPU 密集型
    p3 = Process(name="P3", burst_time=7, io_operations=3)    # I/O 密集型
    p4 = Process(name="P4", burst_time=20)                    # CPU 密集型
    p5 = Process(name="P5", burst_time=5)                     # 短作业
    
    # 添加进程到 MLFQ
    mlfq.add_process(p1)
    mlfq.add_process(p2)
    mlfq.add_process(p3)
    mlfq.add_process(p4)
    mlfq.add_process(p5)
    
    # 运行调度算法
    print("=== Multiple Processes Test ===")
    mlfq.run()
    
    
# 测试6 所有进程优先级相同
# 该测试模拟所有进程都在一个优先级队列中，验证轮转调度是否正常工作
def test_same_priority():
    mlfq = MLFQ(time_quantum=4, max_priority=3)
    
    # 创建进程，所有进程都在同一优先级队列
    p1 = Process(name="P1", burst_time=8)
    p2 = Process(name="P2", burst_time=10)
    p3 = Process(name="P3", burst_time=5)
    
    # 添加进程到 MLFQ
    mlfq.add_process(p1)
    mlfq.add_process(p2)
    mlfq.add_process(p3)
    
    # 运行调度算法
    print("=== Same Priority Test ===")
    mlfq.run()

# 测试7 避免饥饿现象
# 该测试模拟了一个可能导致饥饿现象的情况，验证MLFQ能否通过定期提高优先级来避免进程长时间无法执行
def test_avoid_starvation():
    mlfq = MLFQ(time_quantum=3, max_priority=3)
    
    # 创建进程，其中 P1 的 CPU 时间较长，P2 和 P3 时间较短
    p1 = Process(name="P1", burst_time=80)
    p2 = Process(name="P2", burst_time=5)
    p3 = Process(name="P3", burst_time=4)
    
    # 添加进程到 MLFQ
    mlfq.add_process(p1)
    mlfq.add_process(p2)
    mlfq.add_process(p3)
    
    # 运行调度算法
    print("=== Avoid Starvation Test ===")
    mlfq.run()


if __name__ == "__main__":
    # 执行测试
    print("===↓↓↓↓↓↓↓↓↓ Normal Running Begin ↓↓↓↓↓↓↓↓↓===")
    test_mlfq()
    print("===↑↑↑↑↑↑↑↑↑ Normal Running End ↑↑↑↑↑↑↑↑↑===")

    # 测试1 基本测试
    print("===↓↓↓↓↓↓↓↓↓ Normal Testing Begin ↓↓↓↓↓↓↓↓↓===")
    test_basic_scenario()
    print("===↑↑↑↑↑↑↑↑↑ Normal Testing End ↑↑↑↑↑↑↑↑↑===")
    
    # 测试2 I/O 操作测试
    print("===↓↓↓↓↓↓↓↓↓ IO TESTING Begin ↓↓↓↓↓↓↓↓↓===")
    test_io_operations()
    print("===↑↑↑↑↑↑↑↑↑ IO TESTING End ↑↑↑↑↑↑↑↑↑===")
    
    # 测试3 长CPU占用测试
    print("===↓↓↓↓↓↓↓↓↓ Long CPU TESTING Begin ↓↓↓↓↓↓↓↓↓===")
    test_long_cpu_burst()
    print("===↑↑↑↑↑↑↑↑↑ Long CPU TESTING End ↑↑↑↑↑↑↑↑↑===")
    
    # 测试4 短作业测试
    print("===↓↓↓↓↓↓↓↓↓ Short Task Testing Begin ↓↓↓↓↓↓↓↓↓===")
    test_short_jobs()
    print("===↑↑↑↑↑↑↑↑↑ Short Task Testing End ↑↑↑↑↑↑↑↑↑===")

    # 测试5 多进程、多优先级测试
    print("===↓↓↓↓↓↓↓↓↓ Multi-Process Testing Begin ↓↓↓↓↓↓↓↓↓===")
    test_multiple_processes()
    print("===↑↑↑↑↑↑↑↑↑ Multi-Process Testing End ↑↑↑↑↑↑↑↑↑===")
    
    # 测试6 所有进程都有相同的优先级
    print("===↓↓↓↓↓↓↓↓↓ SamePriority Testing Begin ↓↓↓↓↓↓↓↓↓===")
    test_same_priority()
    print("===↑↑↑↑↑↑↑↑↑ SamePriority Testing End ↑↑↑↑↑↑↑↑↑===")

    # 测试7 避免饥饿现象
    print("===↓↓↓↓↓↓↓↓↓ AvoidStarvation Testing Begin ↓↓↓↓↓↓↓↓↓===")
    test_avoid_starvation()
    print("===↑↑↑↑↑↑↑↑↑ AvoidStarvation Testing End ↑↑↑↑↑↑↑↑↑===")
    


