import numpy as np
import os
from pyspark.mllib.linalg.distributed import *
from pyspark.sql import SparkSession
"""
Enviroment : Apache Spark 2.0
Python version : Python 2.x
Enviroment Resource : http://pythonsparkhadoop.blogspot.com/
"""

class Generate_matrix():
    def __init__(self,seed):
        self.seed = seed
        self.generate()
    
    def generate(self):
        np.random.seed(self.seed)  # seed for reproducibility

        A = np.random.randint(100, size=(5,5))  # Two-dimensional array
        B = np.random.randint(100, size=(5,5))  # Two-dimensional array

        A1 = np.matrix(A)
        np.savetxt('A.out', A1, delimiter=',', fmt='%d') 
        B1 = np.matrix(B)
        np.savetxt('B.out', B1, delimiter=',', fmt='%d') 


class Matrix_multiply():
    def __init__(self, matrix_a_path, matrix_b_path):
        self.matrix_a_path = matrix_a_path
        self.matrix_b_path = matrix_b_path

        self.set_matrix()
        self.zero_matrices()
        self.numpy_matrix_multiply()
        self.regulate_matrix()
        self.matrix_to_list()
        self.encoding_matrix()
        self.RDD_matrix()

    def set_matrix(self):
        self.matrix_a = self.get_matrices(self.matrix_a_path)
        self.matrix_b = self.get_matrices(self.matrix_b_path)

    def get_matrices(self,path):
        elements = []
        with open(path) as file:
            for line in file:
                line=line.strip().split(',')
                elements.append(line)
        file.close()
        return np.array(elements).astype(np.integer)

    def zero_matrices(self):
        self.final_matrix_a = np.zeros(shape=(len(self.matrix_a)*len(self.matrix_a[0]),3))
        self.final_matrix_b = np.zeros(shape=(len(self.matrix_b)*len(self.matrix_b[0]),3))

    def numpy_matrix_multiply(self):
        self.numpy_matrix_multiply = np.dot(self.matrix_a,self.matrix_b)

    def regulate_matrix(self):
        self.final_matrix_a = self.regulate(self.final_matrix_a, self.matrix_a)
        self.final_matrix_b = self.regulate(self.final_matrix_b, self.matrix_b)

    def regulate(self,final_matrix,matrix):
        value = 0
        counter = 1
        for i in range(len(matrix)*len(matrix[0])):
            final_matrix[i][0] = value
            final_matrix[i][1] = i%len(matrix[0])
            final_matrix[i][2] = matrix[value][i%len(matrix[0])]
            if(i != 0 and counter%len(matrix[0]) == 0):
                value += 1
            counter += 1
        return final_matrix

    def matrix_to_list(self):
        self.final_matrix_a = self.final_matrix_a.tolist()
        self.final_matrix_b = self.final_matrix_b.tolist()
        
    def encoding_matrix(self):
        self.final_matrix_a = self.encoding(self.final_matrix_a, len(self.matrix_a)*len(self.matrix_a[0]))
        self.final_matrix_b = self.encoding(self.final_matrix_b, len(self.matrix_b)*len(self.matrix_b[0]))

    def encoding(self, matrix, length):
        for i in range(length):
            matrix[i] = MatrixEntry(matrix[i][0],matrix[i][1],matrix[i][2])
        return matrix

    def RDD_matrix(self):
        RDD1 = CoordinateMatrix(sc.parallelize(self.final_matrix_a)).entries.map(lambda entry: (entry.j, (entry.i, entry.value)))
        RDD2 = CoordinateMatrix(sc.parallelize(self.final_matrix_b)).entries.map(lambda entry: (entry.i, (entry.j, entry.value)))

        matrix_entries = RDD1.join(RDD2).values().map(
            lambda x: ((x[0][0], x[1][0]), x[0][1] * x[1][1])
        ).reduceByKey(
            lambda x, y: x + y
        ).map(
            lambda x: MatrixEntry(x[0][0], x[0][1], x[1])
        )
        matrix_entries.collect()
        matrix = CoordinateMatrix(matrix_entries)
        self.result_rows = matrix.numRows()  
        self.result_cols = matrix.numCols()  
        self.result_list = matrix.entries.collect()
        
        self.refact_result()
        print self.result

    def refact_result(self):
        self.result = np.zeros(shape=(self.result_rows,self.result_cols))
        for i in self.result_list:
            self.result[i.i][i.j] = i.value
        

if __name__ == "__main__":
    _ = Generate_matrix(0)
    _ = Matrix_multiply('A.out','B.out')




