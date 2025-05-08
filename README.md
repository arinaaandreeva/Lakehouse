

## To Run

    docker build -t lakehouse-toy-spark .
    docker run -it --rm lakehouse-toy-spark

## Files

      | data
      |--  podcasts.csv          # data from https://www.kaggle.com/competitions/playground-series-s5e4/data 
      |src
      |-- spark_app.py           # Data preparation + model training
      |-- runner.sh              # to run code 
