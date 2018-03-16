import os

directory = "cabspottingdata"
fileNames = []
for file in os.listdir(directory):

    if file.startswith("new_"):
        fileNames.append(file)
output = open("cabdata.txt" ,"w")
for file in fileNames :

    with open(directory+"/"+file) as f :
        for line in f :
            newLine = file[4:-4]+" "+ line

            output.write(newLine)

output.close()


