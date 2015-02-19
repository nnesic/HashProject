from Tkinter import Tk, Canvas, Frame, BOTH


class Node(object):
    pass

class Ring(object):

    def __init__():
        self.nodes = []


    def addNode(self, node):
        self.nodes += [node]

    def removeNode(self, node):
        print ("not implemented yet")




class DreawIt(Frame):
  
    def __init__(self, parent):
        Frame.__init__(self, parent)   
         
        self.parent = parent        
        self.initUI()
        
    def initUI(self):
      
        self.parent.title("Colors")        
        self.pack(fill=BOTH, expand=1)

        canvas = Canvas(self)
        canvas.create_rectangle(30, 10, 120, 80, 
            outline="#fb0", fill="#fb0")
        canvas.create_rectangle(150, 10, 240, 80, 
            outline="#f50", fill="#f50")
        canvas.create_rectangle(270, 10, 370, 80, 
            outline="#05f", fill="#05f")            
        canvas.pack(fill=BOTH, expand=1)


def main():
  
    root = Tk()
    ex = DrawIt(root)
    root.geometry("400x100+300+300")
    root.mainloop()  


if __name__ == '__main__':
    main()  
