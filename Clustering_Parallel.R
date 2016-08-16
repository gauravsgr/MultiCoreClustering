library(sqldf)
library(dplyr) 
library(RODBC)
library(foreach)
library(doSNOW) # change the package if on Linux
library(ggplot2)

setwd('C:\\R_Working_Dir\\OutputData')

# Function to create the Cluster Definition table
pt25 = function(x) {return (quantile(x, 0.25))}
pt75 = function(x) {return (quantile(x, 0.75))}
cluster.Def = function(df,...){
  return (t(df %>% group_by(...) %>% summarise_each(funs(pt25, median, pt75))))
}

# Function to implement the scree plot utilizing all the cores of the system
plotParallelScree = 
  function (df, cores, seedvector, prefix = ""){
    # Setting the up the cores
    cl = makeCluster(cores)
    registerDoSNOW(cl)
    cat(paste(getDoParWorkers(),"Parallel thread intiated successfully!"))
    
    x = seedvector
    wss0 <- (nrow(df)-1)*sum(apply(df,2,var))  
    wss <- 
      foreach(j=1:length(x), .combine='cbind') %:%
        foreach(i=2:18, .combine = "c") %dopar% {
          set.seed(x[j]);  sum(kmeans(df, centers=i, iter.max = 65532)$withinss)
        }
    for(j in 1:length(x)){ 
      a = paste("Elbow Curve - Kmeans (seed = ",x[j],")", sep = "")
      png(file = paste(prefix,"Cluster_seed",x[j] ,".png",sep = ""), height=630, width = 864)
      plot(1:18, c(wss0,wss[,j]), main = a, type="b", xlab="Number of Clusters", ylab="Average within - cluster sum of squares",col = "royalblue")                                                                                                                                                                                               
      dev.off()
    } 
    stopCluster(cl)
    print(paste("Knee elbow plots created successfully at",getwd()))
    return (0)
  } 


#Reading the Datafile                                                                                                                                                                                    
dbhandle <- odbcDriverConnect('driver={SQL Server};server=completeServerName;database=databaseName;trusted_connection=true')                
rawdata <- sqlQuery(dbhandle,"select * from  database.schema.TableName")

as.data.frame(colnames(rawdata))
# Capping the Units and the Utilized Revenue                                                                                                                                                                                                                                                                                                                                                                          
newdf = as.data.frame(scale(rawdata[,c(3:7)]))


newdf$var1 = ifelse(newdf$var1 > 3, 3, newdf$var1)
newdf$var1 = ifelse(newdf$var1 < -3, -3, newdf$var1)

# Removing the slope and dips
newdf = newdf[c(-2,-5,-6,-9)] 

data.frame(colnames(newdf))
# Getting the optimal no. of clusters    
as.data.frame(colnames(newdf))
x = c(seq(1951, 1990, 2))
x = c(seq(1971, 1990, 2))
plotParallelScree(newdf, 4, x, prefix = "Iteration1")

wss <- (nrow(newdf)-1)*sum(apply(newdf,2,var))   
for(j in 1:length(x)){
  for (i in 2:18) {set.seed(805);  wss[i] <- sum(kmeans(newdf, centers=i, iter.max = 900)$withinss)}   
  a = paste("Elbow Curve - Kmeans (seed = ",807,")", sep = "")
  plot(1:18, wss, main = a, type="b", xlab="Number of Clusters", ylab="Average within - cluster sum of squares")                                                                                                                                                                                               
}
rawdata = rawdata[-10]
set.seed(1989); rawdata$ClusterId = kmeans(newdf, centers=8, iter.max = 65532)$cluster                                                                                                                                                                                             
final = sqldf('select a.*, ClusterId from cdata2 b, rawdata a where a.Customer = b.Customer')

table(rawdata$ClusterId)
set.seed(790); fit = kmeans(newdf, centers=5, iter.max = 900)

# Getting the cluster defintion                                                                                                                                                                                   
summary = cluster.Def(rawdata[,-1], ClusterId)
sqldf('select ClusterId, SUM(HigherFreeUsage), count(*) from rawdata Group by ClusterId')
write.table(summary, "clustersummary.txt", sep=",",row.names=TRUE) 
t(a)

data.frame(table(final[,14]))

# Creating 2d plots for all the variables
for (x in 1:dim(newdf)[2]-1){
  for(y in 1:dim(newdf)[2]){
    if(x <= y) next
    png(file = paste(colnames(newdf)[y],"Vs",colnames(newdf)[x],".png",sep = ""),height=680,width=680)
    plot(unlist(newdf[x]), unlist(newdf[y]), col=((unlist(newdf[dim(newdf)[2]]))-1)*25+12,pch=15,cex=1.5, main = paste(colnames(newdf)[x],"Vs",colnames(newdf)[y]), xlab = colnames(newdf)[x], ylab = colnames(newdf)[y])
    dev.off()
  }
}
while(dev.off()!=NULL){}

# In future, Look at the principal components as see if things are clustered well
data.frame(colnames(newdf))
write.table(rawdata, "rawdataOutput.txt", quote = FALSE, na = "", row.names = FALSE, sep="\t")          

