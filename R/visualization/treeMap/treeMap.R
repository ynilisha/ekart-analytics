################### Create tree map start #############
setwd("E:/eKart/visualization/treeMap/")

library(treemap)

# Read files which contain the hub data and colour mapping
treeMap = read.csv("hub_load_utilization.csv")
colourMapping<-read.csv("colorMapping.csv")

treeMap=merge(treeMap, colourMapping, by = c("utilization"), all.x = TRUE)
treeMap$color = as.character(treeMap$color)
treeMap$load = as.numeric(treeMap$load)

png("hubUtilization.png",    # create PNG for the heat map        
    width = 7*300,        # 5 x 300 pixels
    height = 5*300,
    res = 300,            # 300 pixels per inch
    pointsize = 8)        # smaller font size

treemap(treeMap, index = "hub", vSize = "load", vColor = "color", type = "color", title = "Hub wise utilization", 
        lowerbound.cex.labels = 0.2, 
        #overlap.labels = 1, 
        #force.print.labels = FALSE, 
        #inflate.labels = TRUE, 
        fontsize.labels = 8 )

dev.off()   
################### Create tree map end #############
