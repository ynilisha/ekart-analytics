# =========================================================
#  load_packing_raw_data()
# =========================================================

load_packing_raw_data <- function(working_dir=
             "C:/FC Analytics/Projects/Packing optimizaiton/Pack_optimization",
             file ="packing_raw.csv",
             warehouse='All',
             cms_vertical='All')
{
  setwd(working_dir)
  packing_raw <- read.csv(file,stringsAsFactors=F)
  
  packing_raw <- na.omit(packing_raw)
  
  names(packing_raw) <- c('fc','cv','ct','lorig','borig','horig','sbox','ubox','ubucket','month','shipments')
  
  packing_raw$l <- as.numeric(packing_raw$lorig)+as.numeric(packing_raw$ct)
  packing_raw$b <- as.numeric(packing_raw$borig)+as.numeric(packing_raw$ct)
  packing_raw$h <- as.numeric(packing_raw$horig)+as.numeric(packing_raw$ct)
 
  if(warehouse!= 'All'){
    packing_raw <- subset(packing_raw, fc %in% warehouse)
  }
  
  if(cms_vertical != 'All')  {
    packing_raw <- subset(packing_raw,cv %in% cms_vertical)
  }
  
  print(paste(nrow(packing_raw[-1,]),'rows of packing raw data corresponding to',sum(packing_raw$shipments),'shipments loaded for',warehouse)) 
  return(packing_raw[-1,])
}

# =========================================================
#  load_packing_box_data()
# =========================================================

load_packing_box_data <- function(working_dir=
                                    "C:/FC Analytics/Projects/Packing optimizaiton/Pack_optimization",
                                  file ="packing_boxes_all.csv",
                                  warehouse="All")
{
  setwd(working_dir)
  packing_boxes <- read.csv(file,stringsAsFactors=F)
  packing_boxes <- na.omit(packing_boxes)
  names(packing_boxes) <- c('box','bl','bb','bh')
# packing_boxes <- subset(packing_boxes,(box=='SB2' & bl!=10)|box!='SB2')
  packing_boxes <- subset(packing_boxes,as.numeric(bl)>0)
  
  if(warehouse!= 'All'){
    packing_boxes <- subset(packing_boxes, fc %in% warehouse)
  }
  
  packing_boxes <- transform(packing_boxes, bl=as.numeric(bl),
                            bb=as.numeric(bb), bh=as.numeric(bh))
  
  packing_boxes$box_vol <- with(packing_boxes, bl*bb*bh)
  packing_boxes$box_surf_area <- with(packing_boxes, 2*(bl*bb+bb*bh+bh*bl))
  
  print(paste(nrow(packing_boxes),'rows of packing box data loaded for',warehouse)) 
  return(packing_boxes[order(packing_boxes$box_vol),])
}

# =========================================================
#  load_brown_box_data()
# =========================================================

load_brown_box_data <- function(working_dir=
                                    "C:/FC Analytics/Projects/Packing optimizaiton/Pack_optimization",
                                  file ="packing_boxes.csv",
                                min_used=100,
                                warehouse="All")
{
  setwd(working_dir)
  packing_boxes <- read.csv(file,stringsAsFactors=F)
  packing_boxes <- na.omit(packing_boxes)
  names(packing_boxes) <- c('box','packing_bucket','fc','l','b','h','shipments')
  
  packing_boxes <- subset(packing_boxes,as.numeric(l)>0 & packing_bucket %in% c('brown_box','LifystyleBox'))
  
  packing_boxes <- transform(packing_boxes, l=as.numeric(l),
                             b=as.numeric(b), h=as.numeric(h),
                             shipments = as.numeric(shipments))
  
  packing_boxes <- subset(packing_boxes,shipments >= min_used)
  
  packing_boxes$vol <- with(packing_boxes, l*b*h)
  packing_boxes$surf_area <- with(packing_boxes, 2*(l*b+b*h+h*l))
  packing_boxes <- subset(packing_boxes,vol>0)
  
  if(warehouse!= 'All'){
    packing_boxes <- subset(packing_boxes, fc %in% warehouse)
  }
 
  print(paste(nrow(packing_boxes),'rows of brown box data loaded for',warehouse))  
  return(return(packing_boxes[order(packing_boxes$vol),]))
}

# =========================================================
#  process_packing_raw_data()
# =========================================================

process_packing_raw_data <- function(packing_data=packing_raw,
                                     box_data=packing_boxes) 
{
  library(sqldf)
  packing_data <- sqldf('select pr.*, pb.box_vol as svol, pb.box_surf_area as ssa
                       from packing_data pr left join box_data pb on
                       pr.sbox = pb.box')
  
  
  packing_data <- sqldf('select pr.*, pb.box_vol as uvol, pb.box_surf_area as usa
                       from packing_data pr left join box_data pb on
                       pr.ubox = pb.box')
  
  packing_data <- subset(packing_data,cv %in% c('mobile','tablet') | ubucket %in% c('brown_box','LifestyleBox'))
  
  packing_data <- subset(packing_data,!(sbox %in% c('LCL','Box','SB')))
  
  packing_data <- subset(packing_data,l>0 & b>0 & h>0)
  
  packing_data$ship_box <- ifelse(packing_data$cv %in% c('mobile','tablet'),
                                  packing_data$sbox,packing_data$ubox)
  
  packing_data$ship_vol <- ifelse(packing_data$cv %in% c('mobile','tablet'),
                                  packing_data$svol,packing_data$uvol)
  
  packing_data$ship_surf_area <- ifelse(packing_data$cv %in% c('mobile','tablet'),
                                  packing_data$ssa,packing_data$usa)
  
  packing_data <- subset(packing_data, select = -c(cv,sbox,ubox,ubucket,svol,ssa,uvol,usa,ct))
  
  packing_data$lu <- rep(NULL,nrow(packing_data))
  packing_data$bu <- rep(NULL,nrow(packing_data))
  packing_data$hu <- rep(NULL,nrow(packing_data))
  
  for(i in 1:nrow(packing_data)) {
    packing_data[i,'lu'] <- sort(as.numeric(packing_data[i,c('l','b','h')]))[3]
    packing_data[i,'bu'] <- sort(as.numeric(packing_data[i,c('l','b','h')]))[2]
    packing_data[i,'hu'] <- sort(as.numeric(packing_data[i,c('l','b','h')]))[1]
  }
  
  packing_data <- subset(packing_data, select = -c(l,b,h))
  
  names(packing_data) =c('fc','lorig','borig','horig','month','shipments','ship_box','ship_vol','ship_surf_area','l','b','h')
  
  packing_data <- subset(packing_data,ship_vol>0)
  
  print(paste(nrow(packing_data),'rows of packing data remain corresponding to',sum(packing_data$shipments),'shipments after filtering for brown box usage'))  
  
  return(packing_data)
}

# =========================================================
#  calculate_packing_efficiency()
# =========================================================

calculate_packing_efficiency <- function(packing_final=packing_data)
{
  #packing_final <- na.omit(packing_final)
  
  print(paste('Total shipments (in lakhs) :',round(sum(packing_final$shipments)/100000,2)))
  
  packing_final_not_na <- subset(packing_final,!is.na(ship_vol))
    #subset(packing_final,!is.na(box_l))
  
  print(paste('Coverage through selected boxes : ',round(sum(packing_final_not_na$shipments)/sum(packing_final$shipments),2)*100,'%',sep=''))
  
  print(paste('Vol. diff (in lakhs of cubic iches): ',
              with(packing_final_not_na,round(sum((ship_vol-l*b*h)*shipments)/100000,2))))
  
  print(paste('Packing surf. area (in lakhs of square iches): ',
              with(packing_final_not_na,round(sum(ship_surf_area*shipments)/100000,2))))
  
  print(paste('Minimum possible volumetric ratio : ',
              with(packing_final_not_na,round(sum(l*b*h*shipments)/sum(lorig*borig*horig*shipments),2))))
  
  print(paste('Total volumetric ratio : ',
              with(packing_final_not_na,round(sum(ship_vol*shipments)/sum(lorig*borig*horig*shipments),2))))
  
  print(paste('Total volumetric ratio (including tolerance) : ',
              with(packing_final_not_na,round(sum(ship_vol*shipments)/sum(l*b*h*shipments),2))))
  
  #packing_final_vol_ratio <- rep(with(packing_final_not_na,ship_vol/(lorig*borig*horig)),
  #                               packing_final_not_na$shipments)
  
#  print(paste('Vol. ratio percentiles : '))
 # print(round(quantile(packing_final_vol_ratio,seq(0.5,0.9,0.2)),2))
}

# =========================================================
#  GENERATE_PACKING_BOXES()
# =========================================================

#Function to generate all possible packing boxes with LBH 
#being 0.5 in apart from 0 inches to 40 inches

generate_packing_boxes <- function(lmin=6.5,bmin=4.5,hmin=1.5,lmax=25,bmax=25,hmax=25,increment=0.1){
  
  l<-seq(lmin,lmax,increment)
  b<-seq(bmin,bmax,increment)
  h<-seq(hmin,hmax,increment)
  
  packing_box <- expand.grid(l=l,b=b,h=h)
  
  packing_box<- subset(packing_box,l>=b & b>=h)
  
  packing_box <- transform(packing_box, vol = l*b*h)
  
  packing_box <- transform(packing_box, surf_area = 2*(l*b+b*h+h*l))
  
  packing_box <- packing_box[order(packing_box$vol),]
  
  packing_box<-cbind(box=1:nrow(packing_box),packing_box)
  
  print(paste(round(nrow(packing_box)/100000,2),'lakhs new packing box dimensions generated with min lbh of',lmin,',',bmin,',',hmin,'and increment of',increment))  
  
  return(packing_box)
}

# =========================================================
#  SELECT_BOXES()
# =========================================================

# Function to apply feasibility constraints and select 
# appropriate box for each wid

select_boxes <- function(wids=packing_data,
                       packing_box=new_packing_boxes,
                       cum_perc=0.8,
                       min_shipments=100)
{
  library(sqldf)
  wids_updated <- sqldf('select l,b,h,sum(shipments) as shipments
                         from wids group by l,b,h')
  
  wids_updated <- wids_updated[order(-wids_updated$shipments),]
  
  wids_updated$cum_perc <- 
    cumsum(wids_updated$shipments)/sum(wids_updated$shipments)
  
  wids_updated$box_selected <- rep(NA,nrow(wids_updated))
  
  wids_filtered<- wids_updated[wids_updated$cum_perc<=cum_perc,]
  
  print(paste('Using cumulative sales of',cum_perc*100,'% (',sum(wids_filtered$shipments),') to select boxes'))
  
  for(i in 1:nrow(wids_filtered)) {
    
    wids_updated[i,'box_selected'] <- subset(packing_box, 
                                     l>=wids_updated[i,'l'] & 
                                       b>=wids_updated[i,'b'] & 
                                       h>=wids_updated[i,'h'])[1,'box']
    
  }
 
  boxes_selected <- sqldf('select wu.box_selected as box,
                          pb.l ,
                          pb.b ,
                          pb.h ,
                          pb.vol,
                          pb.surf_area,
                          count(wu.box_selected) as wids,
                          sum(wu.shipments) as shipments
                          from wids_updated wu,
                          packing_box pb
                          where wu.box_selected = pb.box
                          group by wu.box_selected,
                          pb.l,pb.b,pb.h,
                          pb.vol,pb.surf_area')
  
  #print(paste(nrow(boxes_selected),'boxes selected'))
  
  #x <- boxes_selected$shipments>=min_shipments
  
  library(rgl)
  #plot3d(boxes_selected$l,boxes_selected$b,boxes_selected$h,col=ifelse(x,1,2)) 
  
  plot3d(boxes_selected$l,boxes_selected$b,boxes_selected$h) 
  
  boxes_selected <- subset(boxes_selected,shipments>=min_shipments)
  
  boxes_selected <- boxes_selected[order(-boxes_selected$shipments),]
  
  boxes_selected$cum_perc <- 
    cumsum(boxes_selected$shipments)/sum(boxes_selected$shipments)
  
  print(paste(nrow(boxes_selected),'boxes remain after filtering out for minimum shipments per box of',min_shipments))
  return(boxes_selected)
}

# =========================================================
#  ASSIGN_SELECTED_BOXES()
# =========================================================

# Function to apply feasibility constraints and select 
# appropriate box for each wid

assign_selected_boxes <- function(wids=packing_data,
                         packing_box=boxes_selected,
                         roundto =0.5)
{
  wids$box_selected <- rep(NA,nrow(wids))
  wids_duplicate <- wids
  
  #packing_box <- round_packing_boxes(packing_box)
  
  packing_box <- transform(packing_box, vol=l*b*h)
  
  packing_box <- packing_box[order(packing_box$vol),]
  
  for(i in 1:nrow(wids)) {
    
    wids[i,'box_selected'] <- subset(packing_box, 
                                             l>=wids_duplicate[i,'l'] & 
                                               b>=wids_duplicate[i,'b'] & 
                                               h>=wids_duplicate[i,'h'])[1,'box']
    }
  
  wids_final <- sqldf('select w.l,w.b,w.h,
                       w.lorig,w.borig,w.horig,
                       pb.box as ship_box,
                       pb.l as box_l,
                       pb.b as box_b,
                       pb.h as box_h,
                       pb.l*pb.b*pb.h as ship_vol,
                       2*(pb.l*pb.b+pb.b*pb.h+pb.h*pb.l) as ship_surf_area,
                       w.shipments
                       from wids w left join packing_box pb
                       on w.box_selected = pb.box')
  
  wids_final <- transform(wids_final, vol_diff = shipments*(ship_vol-l*b*h))
  
  return(wids_final)
}

# =========================================================
#  round_packing_boxes
# =========================================================
round_packing_boxes <- function(pb=packing_boxes,
                                roundto=0.5){
  pb <- transform(pb, l = ceiling(l/roundto)*roundto 
                           , b = ceiling(b/roundto)*roundto
                           , h = ceiling(h/roundto)*roundto)
  
  pb <- sqldf('select distinct l, b, h 
              from pb')
  
  pb <- cbind(box = 1:nrow(pb),pb)
  
  pb <- transform(pb, vol=l*b*h, 
                  surf_area = 2*(l*b+b*h+h*l))
 
  return(pb)
}
# =========================================================
#  PLOT_KMEANS_WSS()
# =========================================================
plot_kmeans_wss <- function(df=boxes_selected,kmin=10,kmax=40){
for (i in 1:kmax) wss[i] <- sum(kmeans(df,centers=i)$withinss)
print(length(wss))
print(length(kmin:kmax))
plot(kmin:kmax, wss[kmin:kmax], type="b", xlab="Number of Clusters",
     ylab="Within groups sum of squares")
}
# =========================================================
#  CLUSTER_BOXES()
# =========================================================

# Function to cluster the boxes by passing the number of clusters K

cluster_boxes <- function(pb=boxes_selected,k=30){
  
 # pb_updated <- pb[rep(seq_len(nrow(pb)),pb$shipments),]
 # cmod <- kmeans(pb_updated[,c('l','b','h')],k)
  cmod <- kmeans(pb[,c('l','b','h')],k)
# pb_updated$cluster <- cmod$cluster
  pb$cluster <- cmod$cluster
  
  #library(rgl)
  #plot3d(pb$l,pb$b,pb$h,col=cmod$cluster) 
  library(plotly)
  p <- plot_ly(pb, x=l, y=b, z=h, type='scatter3d', mode='markers',
               color=cmod$cluster)
  
  print(p)
  
  p <- plot_ly(pb, x=l, y=b, z=h, type='scatter3d', mode='markers',
        size=pb$shipments/1000, color=cmod$cluster,sizemode='diameter')
  
  print(p)
  return(unique(pb))
}

# =========================================================
#  cluster_center_boxes()
# =========================================================

cluster_center_boxes <- function(pb=boxes_selected,k=30,roundto=0.5){
  
  pb_updated <- pb[rep(seq_len(nrow(pb)),pb$shipments),]
  cmod <- kmeans(pb_updated[,c('l','b','h')],k)
  
  centered_boxes <- as.data.frame(ceiling(cmod$centers/roundto)*roundto)
  centered_boxes <- cbind(box=1:nrow(centered_boxes),centered_boxes)
  centered_boxes <- transform(centered_boxes, vol=l*b*h, 
                               surf_area = 2*(l*b+b*h+h*l))
  
  return(centered_boxes[order(centered_boxes$vol),])
}


# =========================================================
#  cluster_percentile_boxes()
# =========================================================

cluster_percentile_boxes <- function(cb=clustered_boxes,perc=0.5,roundto=0.5){
  
  percentile_boxes_l <- ceiling(aggregate(l~cluster,cb,FUN=function(x){quantile(x,perc)})/roundto)*roundto
  percentile_boxes_b <- ceiling(aggregate(b~cluster,cb,FUN=function(x){quantile(x,perc)})/roundto)*roundto
  percentile_boxes_h <- ceiling(aggregate(h~cluster,cb,FUN=function(x){quantile(x,perc)})/roundto)*roundto
  
  percentile_boxes <- sqldf('select length.cluster as box,
                                    length.l,
                                    breadth.b,
                                    height.h
                            from percentile_boxes_l length left join
                            percentile_boxes_b breadth on length.cluster = breadth.cluster left join
                            percentile_boxes_h height on breadth.cluster = height.cluster')
  
  percentile_boxes <- transform(percentile_boxes, vol=l*b*h, 
                              surf_area = 2*(l*b+b*h+h*l))
  
  return(percentile_boxes[order(percentile_boxes$vol),])
}

# =========================================================
#  cluster_wt_avg_boxes()
# =========================================================

cluster_wt_avg_boxes <- function(cb=clustered_boxes,roundto=0.5){
  wt_avg_box <- sqldf('select c.cluster as box,
                sum(c.l*c.shipments)/sum(c.shipments) as l,
                sum(c.b*c.shipments)/sum(c.shipments) as b,
                sum(c.h*c.shipments)/sum(c.shipments) as h
         from cb c
         group by c.cluster')
  
  wt_avg_box <- round_packing_boxes(wt_avg_box)

  return(wt_avg_box)
}


# =========================================================
#  select_top_distinct_boxes()
# =========================================================

select_top_distinct_boxes <- function(boxes_data = boxes_selected, 
                                      pb = new_packing_boxes_0.5,
                                      n =20){
  
  boxes_data <- cbind(boxes_data,box_selected = NA)
  
  for(i in 1:nrow(boxes_data)) {
    
    boxes_data[i,'box_selected'] <- subset(pb, 
                                           l>=boxes_data[i,'l'] & 
                                             b>=boxes_data[i,'b'] & 
                                             h>=boxes_data[i,'h'])[1,'box']
    
    if(length(unique(boxes_data$box_selected))>n) break
  }
  
  boxes_selected <- sqldf('select distinct bd.box_selected as box,
                          pb.l, pb.b, pb.h, pb.vol, pb.surf_area
                          from boxes_data bd, pb
                          where bd.box_selected = pb.box
                          ')
  
  
  return(boxes_selected)
}

# =========================================================
#  return_packing_data_not_selected
# =========================================================

return_packing_data_not_selected <- function(boxes_data,pb){
  
  roundto=0.5
  
  boxes_data <- transform(boxes_data, l = ceiling(l/roundto)*roundto 
                          , b = ceiling(b/roundto)*roundto
                          , h = ceiling(h/roundto)*roundto)
  
  for(i in 1:nrow(boxes_data)) {
    
    boxes_data[i,'box_selected'] <- subset(pb, 
                                           l==boxes_data[i,'l'] & 
                                             b==boxes_data[i,'b'] & 
                                             h==boxes_data[i,'h'])[1,'box']
  }
  
  boxes_selected_na <- subset(boxes_data, is.na(box_selected))
  
  return(boxes_selected_na)
}

# =========================================================
#  cluster_select_boxes
# =========================================================

cluster_select_boxes <- function(wids=packing_data,
                                 pb=new_packing_boxes_0.5,
                                 k=no_of_clusters,
                                 method='within_cluster_perc'){
  
  roundto =0.5
  
  set.seed(100)
   
  selected_boxes <- data.frame(l= rep(NA,k),
                               b= rep(NA,k),
                               h= rep(NA,k)) 
  
  selected_boxes$box_selected <- NA
  
  clustered_boxes <- cluster_boxes(pb=wids,k=k)
  
  clustered_boxes <- clustered_boxes[order(clustered_boxes$cluster,-clustered_boxes$shipments),]
  
  clustered_boxes$cluster_cum_perc <- NA
  
  for(i in 1:k) {
    clustered_boxes[clustered_boxes$cluster == i,'cluster_cum_perc'] <- 
      cumsum(clustered_boxes[clustered_boxes$cluster == i,'shipments'])/
      sum(clustered_boxes[clustered_boxes$cluster == i,'shipments'])
    
    boxes_selected_cluster <- subset(clustered_boxes, cluster == i & cluster_cum_perc>0.75)
    selected_boxes[i,1:3] <- apply(boxes_selected_cluster[,c('l','b','h')],2,function(x) {ceiling(max(x)/roundto)*roundto})
  }
  
  for(i in 1:nrow(selected_boxes)) {
    selected_boxes[i,'box_selected'] <- subset(pb, 
                                               l==selected_boxes[i,'l'] & 
                                                 b==selected_boxes[i,'b'] & 
                                                 h==selected_boxes[i,'h'])[1,'box']
  }
  
  
  boxes_updated <- sqldf('select distinct bu.box_selected as box,
                         pb.l ,
                         pb.b ,
                         pb.h ,
                         pb.vol ,
                         pb.surf_area
                         from selected_boxes bu, pb
                         where bu.box_selected = pb.box')
  
  return(boxes_updated)							  
}

# =========================================================
#  optimize_packing_boxes
# =========================================================

optimize_packing_boxes <- function(working_dir="C:/FC Analytics/Projects/Packing optimizaiton/Pack_optimization",
                                   training_file ="packing_raw_april.csv",
                                   test_file="packing_raw_april.csv",
                                   packing_box_file='packing_boxes_all.csv',
                                   warehouse='blr_wfld',
                                   cms_vertical='All',
                                   lmin=6.5,
                                   bmin=4.5,
                                   hmin=1.5,
                                   lmax=25,
                                   bmax=25,
                                   hmax=25,
                                   cum_perc=0.8,
                                   min_shipments=0,
                                   boxes_selected_file=NA,
                                   no_of_boxes = 30,
                                   no_of_clusters=15,
                                   percentile_used=0.75,
                                   perc_within_cluster=0.75){
  
  setwd(working_dir)
 
  if(!exists(paste(warehouse,'_packing_raw_data',sep='')))  {
    packing_raw_data <- load_packing_raw_data(file=training_file,warehouse=warehouse,cms_vertical=cms_vertical)
   
    assign(paste(warehouse,'_packing_raw_data',sep=''),packing_raw_data, envir = .GlobalEnv)
  }
  else  {
    packing_raw_data <- get(paste(warehouse,'_packing_raw_data',sep=''))  
  }
  
  if(!exists(paste(warehouse,'_packing_box_data',sep='')))  {
    packing_box_data <- load_packing_box_data(file=packing_box_file,warehouse='All') 
    assign(paste(warehouse,'_packing_box_data',sep=''),packing_box_data, envir = .GlobalEnv)
  }
  else{
    packing_box_data <- get(paste(warehouse,'_packing_box_data',sep=''))
  }
  
  if(!exists(paste(warehouse,'_packing_data',sep='')))  {
    packing_data <- process_packing_raw_data(packing_data=packing_raw_data, box_data =packing_box_data)
    assign(paste(warehouse,'_packing_data',sep=''),packing_data, envir = .GlobalEnv)
  }
  else {
    packing_data <- get(paste(warehouse,'_packing_data',sep='')) 
  }
  
  print('Current packing utlization:')
  cat('\n')
  
  calculate_packing_efficiency(packing_data)
  
  cat('\n')
  
  if(!is.na(boxes_selected_file)){
    test_boxes_selected <- read.csv(boxes_selected_file,stringsAsFactors = F)
    assign(paste(warehouse,'_test_boxes',sep=''),test_boxes_selected, envir = .GlobalEnv)
  }
  
  if(exists('test_boxes_selected')){
    final_boxes_selected <- test_boxes_selected
  }
  else{
    
      new_packing_boxes_0.1 <- generate_packing_boxes(increment=0.1,
                                                      lmin=lmin,lmax=lmax,
                                                      bmin=bmin,bmax=bmax,
                                                      hmin=hmin,hmax=hmax)
      assign('new_packing_boxes_0.1',new_packing_boxes_0.1, envir = .GlobalEnv)
    
    if(!exists(paste(warehouse,cum_perc,'_initial_boxes_selected',sep='')))  {
      initial_boxes_selected <- select_boxes(wids=packing_data,packing_box=new_packing_boxes_0.1,min_shipments=min_shipments, cum_perc=cum_perc)
      assign(paste(warehouse,cum_perc,'_initial_boxes_selected',sep=''),initial_boxes_selected, envir = .GlobalEnv)
    }
    else {
      initial_boxes_selected <- get(paste(warehouse,cum_perc,'_initial_boxes_selected',sep=''))
    }
    
      new_packing_boxes_0.5 <- generate_packing_boxes(increment=0.5,
                                                      lmin=lmin,lmax=lmax,
                                                      bmin=bmin,bmax=bmax,
                                                      hmin=hmin,hmax=hmax)
      assign('new_packing_boxes_0.5',new_packing_boxes_0.5, envir = .GlobalEnv)
    
    top_distinct_boxes_selected <- select_top_distinct_boxes(boxes_data=initial_boxes_selected,
                                                             pb = new_packing_boxes_0.5,
                                                             n=no_of_boxes-no_of_clusters)
    
    assign(paste(warehouse,'_top_distinct_boxes_selected',sep=''),top_distinct_boxes_selected, envir = .GlobalEnv)
    
    packing_data_not_selected <- return_packing_data_not_selected(boxes_data=initial_boxes_selected,
                                                                  pb=top_distinct_boxes_selected) 
    
    assign(paste(warehouse,'_packing_data_not_selected',sep=''),packing_data_not_selected, envir = .GlobalEnv)
    
    if(nrow(packing_data_not_selected)>0){
      k<- ifelse(nrow(packing_data_not_selected)>no_of_clusters,no_of_clusters,nrow(packing_data_not_selected))
      cluster_summary_boxes_selected <- cluster_select_boxes(wids=packing_data,
                                                             k=no_of_clusters,
                                                             method='within_cluster_perc')
      
      assign(paste(warehouse,'_cluster_summary_boxes_selected',sep=''),cluster_summary_boxes_selected, envir = .GlobalEnv)
    }
    
    if(exists('cluster_summary_boxes_selected')){
      training_boxes_selected <- sqldf('select box, l, b, h, vol, surf_area from top_distinct_boxes_selected
                                     union all
                                       select box, l, b, h, vol, surf_area from cluster_summary_boxes_selected')   
    }
    else training_boxes_selected <- top_distinct_boxes_selected
    
    assign(paste(warehouse,'_training_boxes_selected',sep=''),training_boxes_selected, envir = .GlobalEnv)
    
    final_boxes_selected <- training_boxes_selected
  }
  
  updated_packing_data <- assign_selected_boxes(wids=packing_data,packing_box=final_boxes_selected)
  
  assign(paste(warehouse,'_updated_packing_data',sep=''),updated_packing_data, envir = .GlobalEnv)
  
  print('Packing utlization after optimized box selection and fitting:')
  cat('\n')
  
  calculate_packing_efficiency(updated_packing_data)
  
  final_boxes_selected <- sqldf("select wf.ship_box, wf.box_l, wf.box_b, wf.box_h, sum(shipments) as shipments, round(sum(ship_vol*shipments)/sum(lorig*borig*horig*shipments),2) as pf from updated_packing_data wf group by wf.ship_box, wf.box_l, wf.box_b, wf.box_h")
  assign(paste(warehouse,'_final_boxes_selected',sep=''),final_boxes_selected, envir = .GlobalEnv)
  
  cat('\n') 
}