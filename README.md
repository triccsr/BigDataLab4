# BigDataLab4
### usage:
```shell
yarn jar knn-1.0-SNAPSHOT-jar-with-dependencies.jar Knn.Knn YOUR_TEST_PATH/iris_test.csv /YOUR_TRAIN_PATH YOUR_OUTPUT_PATH
```
You need to put iris_test.csv in YOUR_TEST_PATH and iris_train.csv in YOUR_TRAIN_PATH before running the jar.
### for example
```shell
yarn jar knn-1.0-SNAPSHOT-jar-with-dependencies.jar Knn.Knn /input/iris_test.csv /train /output
```
iris_test.csv is in /input, iris_train.csv is in /train, and /output is the output directory.

Notice that /output should be automatically generated by the program, and you **MUST NOT** create it before running jar. 