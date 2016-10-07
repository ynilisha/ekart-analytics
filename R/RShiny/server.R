library(shiny)
library(data.table)
library(dplyr)

df<-fread("RShiny.csv")

shinyServer(
  function(input,output) {
#     output$oid1<-renderPrint({input$id1})
#     output$odate1<-renderPrint({input$date1})
#     output$odate2<-renderPrint({input$date2})
    
    output$newPlot<-renderPlot({
      #df2<-df[(df$First_OFD_date>=input$date1) & (df$First_OFD_date<=input$date2)]
      df2<-df[df$Hub_Type %in% input$id1]
      
      x<-as.data.frame((table(df2$OFD_day)/nrow(df2))*100)
      colnames(x)<-c("day_of_week","OFD_perc")
      levels(x$day_of_week)<-c("Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday")
      plot(x$day_of_week,x$OFD_perc,type="n")
      lines(x$day_of_week,x$OFD_perc)
      
      
    })
    
    }
  
  )


