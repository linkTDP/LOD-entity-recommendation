


with open('insert.sql','r') as f, open('mod.sql', 'w') as m:
    for l in f:
        
        a=l.split("'")
        if len (a) > 3:
            string = a[0]+"'"+a[1]
            
            
            for i in range(len(a)-3):
                string=string+"''"+a[i+2]
            string=string+"'"+a[len(a)-1]
        else:
            string=l 
        m.write(string)
    
    