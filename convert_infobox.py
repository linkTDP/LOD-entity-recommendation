

with open('info_smal.nt','r') as f, open('infoboxl.sql', 'w') as m, open('infobox.sql','w') as i:
    maxL=0
    maxE=0
    for line in f:
        l= line.split(" ")
        del l[-1]
        
        #print " ".join(l[2:])
#         print l[0].replace(">'","").replace("'<","").replace("'","''")
        if len(l[2:]) > 1 or ( len(l[2:])==1 and ( '"' in l[2].replace("<","")[0] or "http://dbpedia.org/" not in l[2])):             
            rows=[l[0].replace(">","").replace("<","").replace("'","''"),l[1].replace(">","").replace("<","").replace("'","''")," ".join(l[2:]).replace(">","").replace("<","").replace("'","''")]
            m.write(("INSERT INTO infobox_l VALUES ('"+rows[0]+"','"+rows[1]+"','"+rows[2]+"');\n").encode('utf-8'))
            if maxL < len(rows[2]):
                maxL=len(rows[2])
        elif len(l[2:])==1:
#             print len(l[2:])
            rows=[l[0].replace(">","").replace("<","").replace("'","''"),l[1].replace(">","").replace("<","").replace("'","''"),l[2].replace(">","").replace("<","").replace("'","''")]
            i.write(("INSERT INTO infobox VALUES ('"+rows[0]+"','"+rows[1]+"','"+rows[2]+"');\n").encode('utf-8'))
            if maxE < len(rows[2]):
                maxE=len(rows[2])
                
    print "max lenght entity"
    print maxE
    
    print "max lenght literal"
    print maxL
    
    