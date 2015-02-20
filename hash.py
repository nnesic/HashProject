from Tkinter import Tk, Canvas, Frame, BOTH, Button, W, NW, FLAT, E, TOP, LEFT
import argparse
import random
import math
import sys

class Node(object):
    
    def __init__(self, position, parent=None):
        self.data = []
        self.position = position    #position on the ring
        self.writes_seen = 0
        self.real_node = parent       #if this is just a token, this points to the real node

    def write(self):
        if (self.real_node != None):
            self.real_node.write()
        else:
            self.writes_seen += 1

    def clean_writes(self):
        self.writes_seen = 0

    def get_writes_seen(self):
        return self.writes_seen 

    def get_position(self):
        return self.position



class Ring(object):

    def __init__(self, S, E, N, W, I, Sm):
        self.nodes = []         # real and tokens
        self.real_nodes = []
        self.E = E
        self.I = I
        self.W = W
        self.N = N
        self.Sm = Sm
        self.add_nodes = self.add_nodes_randomly_with_tokens   #method used to add nodes

        self.add_nodes(S)
        self.evaluate()
        # print "real nodes"
        # for node in self.real_nodes:
        #     print "Position: %d writes: %d" %(node.position, node.writes_seen)
        # print "all nodes"
        # for node in self.nodes:
        #     print "Position: %d writes: %d parent position: %d"  %(node.position, node.writes_seen, node.real_node.position if node.real_node != None else -1 )
        self.add_servers_and_evaluate()


    def add_nodes_randomly(self, numNodes):
        for i in range (0, numNodes):
            if (self.count_real_nodes() >= self.Sm):
                break
            next_server = random.randint(0, self.E-1)
            while (self.is_taken(next_server)):
                next_server = random.randint(0, self.E-1)
            self.nodes += [Node(next_server)]

        self.nodes.sort(key=lambda x: x.position, reverse=False)


    def add_nodes_randomly_with_tokens(self, numNodes):
        for i in range (0, numNodes):
            if (self.count_real_nodes() >= self.Sm):
                break
            next_server = random.randint(0, self.E-1)
            while (self.is_taken(next_server)):
                next_server = random.randint(0, self.E-1)
            real_node = Node(next_server)
            self.nodes += [real_node]
            self.real_nodes += [real_node]
            # Now add the tokens
            tokens = 5
            for i in range (0, tokens):
                next_server = random.randint(0, self.E-1)
                while (self.is_taken(next_server)):
                    next_server = random.randint(0, self.E-1)
                self.nodes += [Node(next_server, real_node)]


        self.nodes.sort(key=lambda x: x.position, reverse=False)
        

    def is_taken(self, position):
        for node in self.nodes:
            if node.position == position:
                return True
        return False 


    def write_to_node(self, key_hash):
        min_distance = self.E + 1
        node = -1
        #find the node at which to start writing
        for i in range (0, len(self.nodes)):
            if self.nodes[i].position > key_hash and (self.nodes[i].position - key_hash) < min_distance:
                min_distance = self.nodes[i].position - key_hash
                node = i
        #write the replicas of the record
        if self.nodes[node].real_node != None:
            #if this is a token, we need to find the index of its parent
            parent_position = self.nodes[node].real_node.position
            for i in range (0, len(self.real_nodes)):
                if self.real_nodes[i].position == parent_position:
                    node = i
                    break
        for r in range (0, self.N):
            self.real_nodes[(node + r) % len(self.real_nodes)].write()


    def add_servers_and_evaluate(self):
        while len(self.real_nodes) < self.Sm:
            print "Adding servers"
            self.add_nodes(self.I)
            self.evaluate()


    def evaluate(self):
        for node in self.nodes:
            node.clean_writes()
        for i in range(0, self.W):
            key_hash = random.randint(0, self.E-1)
            self.write_to_node(key_hash)
        self.print_info()


    def count_real_nodes(self):
        return len(self.real_nodes)


    def print_info(self):
        #for node in self.nodes:
            # print "Position: %d writes: %d" % (node.position, node.get_writes_seen())
        #import pdb; pdb.set_trace()
        dev = self.get_standard_deviation()
        desired = self.W * self.N / len(self.real_nodes)
        percentage_offset = dev * 100 / desired
        print "Number of servers: %d Standard deviation: %d (%d%% off desired load)" % (self.count_real_nodes(), dev, percentage_offset)


    def get_standard_deviation(self):
        count = self.count_real_nodes()
        #mean
        mean = 0
        for node in self.real_nodes:
            mean += node.get_writes_seen()
        mean /= count
        #difference of squares
        o = 0
        for node in self.real_nodes:

            o += (node.get_writes_seen() - mean)**2
        o /= count
        return o**0.5

def main():

    #S, E, N, W, I and Sm

    parser = argparse.ArgumentParser(description="parse args")
    parser.add_argument('-S', type=int, help='Initial number of servers', default=10 )
    parser.add_argument('-E', type=int, help='Number of extents', default=10000)
    parser.add_argument('-N', type=int, help='Number of replicas', default=3)
    parser.add_argument('-W', type=int, help='Workload', default=1000000)
    parser.add_argument('-I', type=int, help='Server increment', default=5)
    parser.add_argument('-Sm', type=int, help='Maximum number of servers', default=30)
    parser.add_argument('--visual',  help='Show the GUI', action='store_false', default=True)


    args=parser.parse_args()

    ring = Ring(args.S, args.E, args.N, args.W, args.I, args.Sm)
    #ring.evaluate()
    #ring.print_info()



if __name__ == '__main__':
    main()  
