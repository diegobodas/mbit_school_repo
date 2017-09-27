library(rhdfs)
hdfs.init()

hdfspath = "/projects/dev/es/kvbd/FDM2DS/HUMAN_RESOURCES/habilidades1213.csv"
f = hdfs.file(hdfspath,"r")
m = hdfs.read(f, start=0, n = hdfs.ls(hdfspath)$size)
c = rawToChar(m)
data213 = read.table(textConnection(c), sep = ";", header=TRUE)