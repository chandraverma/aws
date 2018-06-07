def getQueries(filename):
    """This function readrs INCREMENTAL SQL FILE data, replaces getdate() and returns list all commands"""
    with open("./tablescripts/"+filename,"r") as ip:
        data=ip.readlines()
        for i in range(len(data)):
            data[i]=data[i].replace("\n"," ")
        data="".join(data).split(";")
    return data
