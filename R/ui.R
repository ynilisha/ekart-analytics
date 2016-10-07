library(shiny)
shinyUI(pageWithSidebar(
   
     headerPanel("Example 1"),
     
     sidebarPanel(
       
       checkboxGroupInput("id1","Choose hub type : ",
                          c("Commercial" = "Commercial",
                            "Residential" = "Residential",
                            "Mixed" = "Mixed" ),selected="Commercial"
                          ),
       
       dateInput("date1","Start Date","2016-07-11",min="2016-07-10",max="2016-08-06"),
       dateInput("date2","End Date","2016-08-07",min="2016-07-10",max="2016-08-06"),
     
       submitButton('Submit')
       
       
       ),
     
     
     
     mainPanel(
       
#        h3('You chose :'),
#        verbatimTextOutput("oid1"),
#        
#        h4('Start date chosen as :'),
#        verbatimTextOutput("odate1"),
#        
#        h4('End date chosen as :'),
#        verbatimTextOutput("odate2"),
       
       plotOutput('newPlot')
       
       )
  ))