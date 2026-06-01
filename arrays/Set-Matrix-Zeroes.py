// given matrix is of n x m dimensions and we have to find the 0 
// in this matrix and make the entire row and column of that 0 as 0

// but the twist is that we can't make this chages 
// one my one cause if we do changes one by one in same arrya then while changes we 
// will the other 0 which will make use to manipulate without reason therfor we will put
// a copy of that arrys and that it forword 

// now the question is now 
// make the entire arrays present in array 0
// then make 0 to all arrys present in same index 

// ok ? 

// now insted if making a copy and making it an 0 issted we can mark the the rows and columns as -1 and then 
// mark the -1 to 0 and now your arryas is ready 


class Solution(object):
    def setZeroes(self, matrix):
        // lets code 
        for ( i in range(len))