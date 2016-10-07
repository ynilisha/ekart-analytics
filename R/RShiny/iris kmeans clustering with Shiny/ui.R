setwd("C:/Users/rupakkumar.t/Desktop/RShiny - training")

library(shiny)

shinyUI(pageWithSidebar(
  headerPanel('Iris k-means clustering'),
  sidebarPanel(
    selectInput('xcol', 'X Variable', names(iris)),
    selectInput('ycol', 'Y Variable', names(iris),
                selected=names(iris)[[2]]),
    sliderInput('clusters', 'Cluster count', 3,
                 min = 1, max = 10)
  ),
  mainPanel(
    plotOutput('plot1')
  )
))