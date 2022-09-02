# We use a 2-tuple query

import time
import os

start = time.time()
os.system("echo 'Hello world'")
end = time.time() - start

print("It took " + str(end) + " seconds")

QUERY = "/data/cikm/SemanticTableSearchDataset/queries/2_tuples_per_query/wikipage_156705.json"
TABLES = "/data/cikm/SemanticTableSearchDataset/"

# Measures average runtime of command from 3 repetitions in seconds
def time_command(command):
    total = 0
    reps = 3

    for i in range(reps):
        start = time.time()
        os.system(command)
        end = time.time()
        total += end - start

    return total / reps

# Evaluate baseline
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_15_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4"
print("BASELINE: " + str(time_command(command)) + " s")

# Evaluate LSH of types pre-filtering
# 15 buckets
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_15_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4 -pf LSH_TYPES"
print("LSH-TYPES, 15 BUCKETS: " + str(time_command(command)) + " s")

# 30 buckets
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_30_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4 -pf LSH_TYPES"
print("LSH-TYPES, 30 BUCKETS: " + str(time_command(command)) + " s")

# 75 buckets
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_75_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4 -pf LSH_TYPES"
print("LSH-TYPES, 75 BUCKETS: " + str(time_command(command)) + " s")

# 150 buckets
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_150_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4 -pf LSH_TYPES"
print("LSH-TYPES, 150 BUCKETS: " + str(time_command(command)) + " s")

# 300 buckets
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_300_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4 -pf LSH_TYPES"
print("LSH-TYPES, 300 BUCKETS: " + str(time_command(command)) + " s")

# Evaluate LSH of embeddings pre-filtering
# 15 buckets
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_15_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4 LSH_EMBEDDINGS"
print("LSH-EMBEDDINGS, 15 BUCKETS: " + str(time_command(command)) + " s")

# 30 buckets
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_30_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4 LSH_EMBEDDINGS"
print("LSH-EMBEDDINGS, 30 BUCKETS: " + str(time_command(command)) + " s")

# 75 buckets
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_75_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4 LSH_EMBEDDINGS"
print("LSH-EMBEDDINGS, 75 BUCKETS: " + str(time_command(command)) + " s")

# 150 buckets
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_75_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4 LSH_EMBEDDINGS"
print("LSH-EMBEDDINGS, 150 BUCKETS: " + str(time_command(command)) + " s")

# 300 buckets
command = "java -Xmx55g -jar target/Thetis.0.1.jar search --search-mode analogous -topK 100 -i /data/cikm/indexes/buckets_300_bandsize_0_2_permutations_15 -q " + QUERY + " -td " + TABLES + " -od /data/cikm/ -t 4 LSH_EMBEDDINGS"
print("LSH-EMBEDDINGS, 300 BUCKETS: " + str(time_command(command)) + " s")
