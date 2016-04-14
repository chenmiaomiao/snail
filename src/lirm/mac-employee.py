fhand = open('mac.txt', 'rb').readlines()
mac_dict = dict()

for line in fhand:
    items = line.split("###")
        
    # print items
    if len(mac_dict) <= 0:
        mac_dict[items[0].strip()] = [items,]
        continue
        # print mac_dict
    elif items[0].strip() in mac_dict.keys():
        mac_dict[items[0].strip()].append(items)
        continue
    
    
    for key in mac_dict.keys():
        card_all = []
        for mac_item in mac_dict[key]:
            card_all.append(mac_item[1].strip())
        if items[1].strip() in card_all:
            mac_dict[key].append(items)
            continue
    
    mac_dict[items[0].strip()] = [items,]
        

# print mac_dict
fhand2 = open('mac_new.txt', 'w')

for key in mac_dict.keys():
    # print mac_dict[key]
    for mac in mac_dict[key]:
        mac_new_line = key+'\t\t'+mac[0]+'\t\t'+mac[1]+'\n'
        fhand2.write(mac_new_line)