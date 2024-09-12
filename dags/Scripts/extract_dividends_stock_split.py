import os

file_list = os.listdir("./stocks_data/")
dividend = []
stocks_split = []
for i in file_list:
    with open("./stocks_data/"+i,'r') as  f:
        print(i)
        for j in f.readlines():
            if 'Dividend' in j:
                dividend.append(";".join(map(str,j.split(';')[1:3])).replace('Dividend','') +';'+i.replace('.csv','')+'\n')
            if 'Stock Splits'in j:                
                stocks_split.append(";".join(map(str,j.split(';')[1:3])).replace('Stock Splits','') +';'+i.replace('.csv','')+'\n')
        

with open('./dividend.csv','w+') as f:
    f.write('dividend;date;stockName\n')
    f.writelines(dividend)
    f.close()

with open('./stocks_splits.csv','w+') as f:
    f.write('stockSplitRatio;date;stockName\n')
    f.writelines(stocks_split)
    f.close()
    
print("Process Complete")