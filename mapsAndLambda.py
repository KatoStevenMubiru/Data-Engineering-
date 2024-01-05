import pyspark

# Initialize SparkContext
sc = pyspark.SparkContext(appName="maps_and_lazy_evaluation_example")

# Song titles list
log_of_songs = [
    "Despacito",
    "Nice for what",
    "No tears left to cry",
    "Despacito",
    "Havana",
    "In my feelings",
    "Nice for what",
    "despacito",
    "All the stars"
]

# Create an RDD from the list of songs
songs_rdd = sc.parallelize(log_of_songs)

# Show the original input data is preserved
print("Original data:", songs_rdd.collect())

# Function to convert strings to lowercase
def convert_song_to_lowercase(song):
    return song.lower()

print(convert_song_to_lowercase("Songtitle"))

# Use the map function to transform the list of songs with the convert_song_to_lowercase function
lowercased_songs_rdd = songs_rdd.map(convert_song_to_lowercase)

# Show the original input data is still mixed case
print("Original data:", songs_rdd.collect())

# Use lambda functions instead of named functions to do the same map operation
lowercased_songs_rdd_lambda = songs_rdd.map(lambda song: song.lower())

# Print the transformed data using named function
print("Transformed data (using named function):", lowercased_songs_rdd.collect())

# Print the transformed data using lambda function
print("Transformed data (using lambda function):", lowercased_songs_rdd_lambda.collect())

# Stop the SparkContext
sc.stop()
