from Tkinter import Tk, Canvas, Frame, BOTH, Button, W, NW, FLAT, E, TOP, LEFT
import argparse
import random
import math
import sys

class Node(object):
    
    def __init__(self, position):
        self.data = []
        self.position = position    #position on the ring
        self.writes_seen = 0

    def write(self):
        self.writes_seen += 1

    def clean_writes(self):
        self.writes_seen = 0

    def get_writes_seen(self):
        return self.writes_seen 

    def get_position(self):
        return self.position



class Ring(object):

    def __init__(self, S, E, N, W, I, Sm):
        self.nodes = []
        self.E = E
        self.I = I
        self.W = W
        self.N = N
        self.Sm = Sm

        self.add_nodes_randomly(S)

        root = Tk()
        self.ex = DrawIt(root, self.E, self)
        #ex.draw_nodes(self.nodes)
        root.geometry("1000x1000")
        root.mainloop()  


    def add_nodes_randomly(self, numNodes):
        for i in range (0, numNodes):
            if (len(self.nodes) >= self.Sm):
                break
            next_server = random.randint(0, self.E-1)
            while (self.is_taken(next_server)):
                next_server = random.randint(0, self.E-1)
            self.nodes += [Node(next_server)]


    def is_taken(self, position):
        for node in self.nodes:
            if node.get_position() == position:
                return True
        return False

    def add_node(self, node):
        self.nodes += [node]

    def add_nodes_in_increment(self):
        self.add_nodes_randomly(self.I)
        self.ex.draw_nodes(self.nodes)


    def write_to_node(self, key_hash):
        min_distance = self.E + 1
        for node in self.nodes:
            for 

    def evaluate(self):
        for node in self.nodes:
            node.clean_writes()
        for i in range(0, self.W):
            key_hash = math.randint(0, self.E-1)






class DrawIt(Frame):
  
    def __init__(self, parent, E, ring):

        Frame.__init__(self, parent)   
        self.parent = parent 
        self.E = E       
        self.ring = ring
        self.radius = 400
        self.window = 1000
        self.canvas = Canvas(self)
        self.initUI()


        
    def initUI(self):
      
        self.parent.title("Colors")        
        self.pack(fill=BOTH, expand=1)
        #canvas = Canvas(self)
        self.draw_circle(self.window/2, self.window/2, self.radius)
        button1 = Button(self.canvas, text = "Add Nodes", command = self.ring.add_nodes_in_increment, anchor= W)
        button1.configure(width = 10, activebackground = "#33B5E5",background = "#33B5E5", relief = FLAT)
        button1_window = self.canvas.create_window(10, 10, anchor=NW, window=button1)
        button1.pack(side=TOP)

        # button2 = Button(self.canvas, text = "", command = self.ring.clear)
        # button2.configure(width = 10, activebackground = "#33B5E5",background = "#33B5E5", relief = FLAT)
        # button2_window = self.canvas.create_window(10, 10, anchor=NW, window=button2)
        # button2.pack(side=TOP)

        self.draw_nodes(self.ring.nodes)

        self.canvas.pack(fill=BOTH, expand=1)


    def draw_circle(self, x, y, r, **kwargs):
        self.canvas.create_oval(x-r, y-r, x+r, y+r, **kwargs)

    def draw_nodes(self, positions):
        for node in positions:
            self.draw_node(node)

    def draw_node(self, node):
        position = node.get_position()
        y = math.sin(position * math.pi * 2 / self.E) * self.radius
        x = math.cos(position * math.pi * 2 / self.E) * self.radius
        rad = 10
        self.draw_circle(self.window/2 + x, self.window/2 + y, rad, fill="#05f")

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




if __name__ == '__main__':
    main()  
