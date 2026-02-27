from pyspark.sql import SparkSession

def main():
    
    spark = SparkSession.builder \
        .appName("Sherlock Holms Analytics App") \
        .getOrCreate()
    sc = spark.sparkContext
    
    try:
        rdd = sc.textFile("miniproject/*.txt")

        #T1/T2
        lowercase = rdd.map(lambda x: str.lower(x))

        blank_lines = sc.accumulator(0)
        def to_words(line):
            if line == '' or line == ' ':
                blank_lines.add(1)

            words = line.split(' ')
            for i in range(len(words)):
                words[i] = ''.join(char for char in words[i] if char.isalnum())
            return words

        words = lowercase.flatMap(to_words).filter(lambda x: x != '')
        print("Task 1/2: Normalized + word tokenization")
        print(words.take(10))
        print("")

        #T3
        stop_words = sc.broadcast(["the", "a", "an", "is"])
        no_stop_words = words.filter(lambda x: x not in stop_words.value)
        print("Task 3: Removed Stop words")
        print(no_stop_words.take(10))
        print("")

        #T4
        character_count = rdd.map(lambda x: len(x)).sum()
        print("Task 4: Total character count")
        print(character_count)
        print("")

        #T5
        longest_line = rdd.map(lambda x: (x, len(x))).max(lambda x: x[1])
        print("Task 5: Longest line")
        print(longest_line)
        print("")

        #T6
        only_watson = rdd.filter(lambda x: "Watson" in x)
        print("Task 6: Only lines with Watson")
        print(only_watson.take(5))
        print("")

        #T7
        unique = words.map(lambda x: str.lower(x)).distinct()
        print("Task 7: Unique words")
        print(unique.take(5))
        print("")

        #T8
        most_common = words.map(lambda x: (str.lower(x), 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], ascending=False)
        print("Task 8: Most common words")
        print(most_common.take(5))
        print("")

        #T9
        def first_word(line):
            word = line.split(' ')[0]
            return ''.join(char for char in word if char.isalnum())

        first_words_per_line = rdd.map(first_word).filter(lambda x: x != '')
        first_line_word_count = first_words_per_line.map(lambda x: (str.lower(x), 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], ascending=False)

        print("Task 9: First word of each line count")
        print(first_line_word_count.take(5))
        print("")

        #t10
        word_lengths = words.map(lambda x: len(x))
        average_length = word_lengths.sum()/word_lengths.count()
        print("Task 10: Average word length")
        print(average_length)
        print("")

        #T11
        word_length_count = word_lengths.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a+b)
        word_length_count.saveAsTextFile("miniproject/Holmes_WordCount_Results")
        print("Task 11: Word counts + full results saved to text file")
        print(word_length_count.take(5))
        print("")

        #T12
        rdd_with_index = rdd.zipWithIndex()
        start = rdd_with_index.filter(lambda x: x[0] == "ADVENTURE I. A SCANDAL IN BOHEMIA").collect()[0][1]
        end = rdd_with_index.filter(lambda x: x[0] == "ADVENTURE II. THE RED-HEADED LEAGUE").collect()[0][1]

        bohemia = rdd_with_index.filter(lambda x: x[1] >= start and x[1] < end).map(lambda x: x[0])
        print("Task 12: Only the first story")
        print(bohemia.take(5))
        print("")
        
        #O4
        letter_frequency = rdd.flatMap(lambda x: x.split(' ')).flatMap(lambda x: x).map(lambda x: (x,1)).reduceByKey(lambda a,b:a+b)
        print("Task 04: letter frequency")
        print(letter_frequency.take(5))
        print("")
        

        #O5
        letter_group = words.map(lambda x: (x[0], 1)).reduceByKey(lambda a,b:a+b)
        print("05: Letter groupings")
        print(letter_group.take(5))
        print("")
        

    except Exception as e:
        print("An error has occured ",e)
        return None
    spark.stop()

if __name__ == "__main__":
    main()