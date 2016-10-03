################## Map state #########
library(raster)
library(rgdal)
library(rgeos)
library(ggplot2)
library(dplyr)
library(RColorBrewer)

# Working directory
setwd("E:/eKart/visualization/IndiaMap_state/")

### Get data for India
india <- getData("GADM", country = "India", level = 1)

### SPDF to DF
map <- fortify(india)

map$id <- as.integer(map$id)

dat <- data.frame(id = 1:(length(india@data$NAME_1)), state = india@data$NAME_1)
map_df <- inner_join(map, dat, by = "id")

centers <- data.frame(gCentroid(india, byid = TRUE))
centers$state <- dat$state


### This is hrbrmstr's own function
theme_map <- function (base_size = 12, base_family = "") {
   theme_gray(base_size = base_size, base_family = base_family) %+replace% 
      theme(
         axis.line=element_blank(),
         axis.text.x=element_blank(),
         axis.text.y=element_blank(),
         axis.ticks=element_blank(),
         axis.ticks.length=unit(0.3, "lines"),
         #         axis.ticks.margin=unit(0.5, "lines"),
         axis.title.x=element_blank(),
         axis.title.y=element_blank(),
         legend.background=element_rect(fill="white", colour=NA),
         legend.key=element_rect(colour="white"),
         legend.key.size=unit(1.5, "lines"),
         legend.position="right",
         legend.text=element_text(size=rel(1.2)),
         legend.title=element_text(size=rel(1.4), face="bold", hjust=0),
         panel.background=element_blank(),
         panel.border=element_blank(),
         panel.grid.major=element_blank(),
         panel.grid.minor=element_blank(),
         panel.margin=unit(0, "lines"),
         plot.background=element_blank(),
         plot.margin=unit(c(1, 1, 0.5, 0.5), "lines"),
         plot.title=element_text(size=rel(1.8), face="bold", hjust=0.5),
         strip.background=element_rect(fill="grey90", colour="grey50"),
         strip.text.x=element_text(size=rel(0.8)),
         strip.text.y=element_text(size=rel(0.8), angle=-90) 
      )   
}

# state wise data with hue
state = read.csv("states.csv")
hue = read.csv("hue.csv")
map_df2 = merge(map_df, state, by = c("state"))
map_df2 = merge(map_df2, hue, by = c("metric"))
map_df2 = map_df2[order(map_df2$order),]

# create hue map
hueMap = as.data.frame(hue[1,])
for(i in 2:nrow(hue)) 
   if(hue$hue[i]!=hue$hue[i-1]) hueMap = rbind(hueMap,hue[i,])


# create the plot
png("statePerformance.png",    # create PNG for the heat map        
    width = 7*300,        # 5 x 300 pixels
    height = 5*300,
    res = 300,            # 300 pixels per inch
    pointsize = 8)        # smaller font size

xl <- 1
yb <- 1
xr <- 1.5
yt <- 2


layout(matrix(1:2,nrow=1),widths=c(0.8,0.2))
par(mar=c(5.1,4.5,4.1,2.1))

ggplot() +
   geom_map(data = map_df2, map = map_df2,
            aes(map_id = id, x = long, y = lat, group = group), 
            color = "#ffffff", fill = map_df2$hue, size = 0.25) +
   #   geom_text(data = centers, aes(label = state, x = x, y = y), size = 2) +
   coord_map() +
   labs(x = "", y = "", title = "State wise metric performance") +
   theme_map() +
   theme(legend.position = "none")

# Margins need to be fixed...legend is overriding the map 
# par(mar=c(5.1,4.5,4.1,0.5))
# plot(NA,type="n",ann=FALSE,xlim=c(1,2), ylim=c(1,2),xaxt="n",yaxt="n",bty="n")
# rect(
#    xl,
#    head(seq(yb,yt,(yt-yb)/9),-1),
#    xr,
#    tail(seq(yb,yt,(yt-yb)/9),-1),
#    col=as.character(hueMap$hue)
# )
# 
# mtext(hueMap$Metric..[1:9],side=2,at=tail(seq(yb,yt,(yt-yb)/9),-1)-0.05,las=2,cex=0.7)

dev.off()  

#################### Map End ####################


library('data.table')

dataR = fread("July_extract_ekart.csv")
dataR$quantity = 1
orders_state = aggregate(dataR$quantity, by = list(dataR$destination_state), FUN = sum)
colnames(orders_state) = c("state", "orders")

payment_state = aggregate(dataR$quantity, by = list(dataR$destination_state, dataR$payment_type), FUN = sum)
colnames(payment_state) = c("state", "payment_type", "orders")

seller_state = aggregate(dataR$quantity, by = list(dataR$destination_state, dataR$seller_type), FUN = sum)
colnames(payment_state) = c("state", "seller", "orders")

RTO_state = aggregate(dataR$quantity, by = list(dataR$destination_state, dataR$ekl_shipment_type), FUN = sum)
colnames(payment_state) = c("state", "shipment_type", "orders")

BU_state = aggregate(dataR$quantity, by = list(dataR$destination_state, dataR$analytic_business_unit), FUN = sum)
colnames(payment_state) = c("state", "BU", "orders")

dataR$value_bucket = ifelse(dataR$shipment_value <300, "L300", ifelse(dataR$shipment_value <1000, "L1000", ifelse(dataR$shipment_value <5000, "L5000", "G5000")))
valueBucket_state = aggregate(dataR$quantity, by = list(dataR$destination_state, dataR$value_bucket), FUN = sum)
colnames(payment_state) = c("state", "value_bucket", "orders")

dataR$LZN_bucket = ifelse(dataR$new_lzn == "L1" | dataR$new_lzn == "L2" | dataR$new_lzn == "Z1", "LZ1", ifelse(dataR$new_lzn == "N1" | dataR$new_lzn == "N2", "N", ifelse(dataR$new_lzn == "Z2", "Z2", "")))
lznBucket_state = aggregate(dataR$quantity, by = list(dataR$destination_state, dataR$LZN_bucket), FUN = sum)
colnames(payment_state) = c("state", "LZN_bucket", "orders")
