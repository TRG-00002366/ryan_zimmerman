from pyspark.sql import SparkSession

def main():
    
    spark = SparkSession.builder \
        .appName("Sherlock Holms Analytics App") \
        .getOrCreate()
    sc = spark.sparkContext
    
    rdd = sc.textFile("miniproject/sherlock_holms.txt")

    #T1
    lowercase = rdd.map(lambda x: str.lower(x))

    #T2
    def to_words(line):
        words = line.split(' ')
        for i in range(len(words)):
            words[i] = ''.join(char for char in words[i] if char.isalnum())
        return words

    words = rdd.flatMap(to_words).filter(lambda x: x != '')

    #T3
    no_stop_words = words.filter(lambda x: x not in ["the", "a", "an", "is"])

    #T4
    character_count = rdd.map(lambda x: len(x)).sum()
    
    #T5
    longest_line = rdd.map(lambda x: (x, len(x))).max(lambda x: x[1])

    #T6
    only_watson = rdd.filter(lambda x: "Watson" in x)

    #T7
    unique = words.map(lambda x: str.lower(x)).distinct()

    #T8
    most_common = words.map(lambda x: (str.lower(x), 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], ascending=False)

    #T9
    def first_word(line):
        word = line.split(' ')[0]
        return ''.join(char for char in word if char.isalnum())
        
    first_words_per_line = rdd.map(first_word).filter(lambda x: x != '')
    first_line_word_count = first_words_per_line.map(lambda x: (str.lower(x), 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], ascending=False)

    #t10
    word_lengths = words.map(lambda x: len(x))
    average_length = word_lengths.sum()/word_lengths.count()

    #T11
    word_length_count = word_lengths.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)

    #T12
    rdd_with_index = rdd.zipWithIndex()
    start = rdd_with_index.filter(lambda x: x[0] == "ADVENTURE I. A SCANDAL IN BOHEMIA").collect()[0][1]
    end = rdd_with_index.filter(lambda x: x[0] == "ADVENTURE II. THE RED-HEADED LEAGUE").collect()[0][1]

    bohemia = rdd_with_index.filter(lambda x: x[1] >= start and x[1] < end).map(lambda x: x[0])

    print(bohemia.take(20))
    spark.stop()

if __name__ == "__main__":
    main()