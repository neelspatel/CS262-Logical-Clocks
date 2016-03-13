import redis
import threading
import time
import random
import sys



POOL = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
r = redis.Redis(connection_pool=POOL)


processes = ['one', 'two', 'three']

class Queue(object):
    """An abstract FIFO queue"""
    def __init__(self, queue_name, clear=False):
        #create new queue
        self.id_name = queue_name
        if clear:
            res = self.pop() 
            while res is not None:
                res = self.pop()
            
    def push(self, element):
        """Push an element to the tail of the queue""" 
        id_name = self.id_name
        push_element = r.lpush(id_name, element)

    def pop(self):
        """Pop an element from the head of the queue"""
        id_name = self.id_name
        popped_element = r.rpop(id_name)
        return popped_element

    def length(self):
        id_name = self.id_name
        queue_len = r.llen(id_name)
        return queue_len

class Machine(object):
    #initializes a machine with a queue and 
    def __init__(self, name):
        self.name = name
        self.speed = 1.0 / random.randint(1,6)
        self.logical_clock = 0
        
        #saves the current system time
        self.system_start_time = int(time.time() * 1000)
        
        #remove the current machine name, and save the other machine names
        processes.remove(self.name)
        assert(len(processes)==2)
        self.other_machine_names = processes
        
        #sets up a local Redis queue
        self.queue = Queue(queue_name="queue_" + self.name, clear=True)
        
        self.queue.push("ready")
        self.queue.push("ready")
        
        #waits for the other queues to push "ready" messages
        ready1 = False
        ready2 = False
        self.q1 = Queue("queue_" + self.other_machine_names[0])
        self.q2 = Queue("queue_" + self.other_machine_names[1])
        
        while not (ready1 and ready2):
            if not ready1:
                msg = self.q1.pop() 
                if msg is not None:
                    print "machine "+self.other_machine_names[0]+" ready"
                    assert(msg == "ready")
                    ready1 = True
            if not ready2:
                msg = self.q2.pop() 
                if msg is not None:
                    print "machine "+self.other_machine_names[1]+" ready"
                    assert(msg == "ready")
                    ready2 = True
        
        print "Ready, at interval", self.speed
        
        #sets up a log file
        self.log = open("log_" + self.name, "w+")
    
        #setup a timer to check the queue
        while True:
            self.check_messages()
            time.sleep(self.speed)
    
    #sends a message to a machine    
    def send_message(self, to_machine):
        to_machine.push(str(self.logical_clock))
    
    #called on an interval, checks for messages in the Redis queue
    #if a message exists, we react to it; otherwise, we have an internal event 
    def check_messages(self):
        print "Checking messages"
        if self.queue.length() > 0:
            print "processing receieved"
            try:
                current_message = int(self.queue.pop())
                 
                #update the logical clock
                self.update_logical_clock(current_message)
                
                #write to the log
                self.write_to_log("received")
            except:
                print "Error reading message"
           
        #otherwise, react to an internal event
        else:
            print "adding new messages"
            #generates a random(1,10) to decide what to do
            message_decision = random.randint(1,10)
            
            #send a message to the first other machine
            if message_decision <= 3:
                if message_decision == 1:
                    self.send_message(self.q2)
                #send a message to the second other machine
                elif message_decision == 2:
                    self.send_message(self.q1)
                #send a message to both other machines
                elif message_decision == 3:
                    self.send_message(self.q2)
                    self.send_message(self.q1)
                
                 #update the logical clock
                self.update_logical_clock()
                
                #write to the log
                self.write_to_log("sent")
            else:
                #update the logical clock
                self.update_logical_clock()
                
                #write to the log
                self.write_to_log("internal")
                    
    
    #updates the logical clock, with the rule of taking the greater time
    def update_logical_clock(self, new_clock_time=None):
        if new_clock_time==None:
            self.logical_clock = self.logical_clock + 1
        elif new_clock_time > self.logical_clock:
            self.logical_clock = new_clock_time
       
    #gets the number of milliseconds elapsed since the beginning of this program based on system time
    def get_system_time(self):
        return int(time.time() * 1000)
        
    #writes to the log for a given action,
    #including the system time and local logical clock time
    def write_to_log(self, action):
        print "Writing to log", action
        #write to log that we recieved, global_time, queue length, logical_clock_time
        if action=="received":
            self.log.write(", ".join(map(str, [
                "received",
                self.get_system_time(),
                self.logical_clock,
                self.queue.length()
            ])) + "\n")
        
        #write to log that we sent, global_time, logical_clock_time
        elif action=="sent":
            self.log.write(", ".join(map(str, [
                "sent",
                self.get_system_time(),
                self.logical_clock
            ])) + "\n")
            
        #write to log that we had an internal event, global_time, logical_clock_time
        elif action=="internal":
            self.log.write(", ".join(map(str, [
                "internal event",
                self.get_system_time(),
                self.logical_clock
            ])) + "\n")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        print "Creating machine '" + sys.argv[1] + "'"
        machine = Machine(sys.argv[1])