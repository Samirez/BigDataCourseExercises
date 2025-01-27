from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode, split, to_json, array, col
import locale

locale.getdefaultlocale()
locale.getpreferredencoding()

# Read in negative and positive words list
positiveWords = ['absolutely', 'accepted', 'acclaimed', 'accomplish', 'accomplishment', 'achievement', 'action',
                 'active', 'admire', 'adorable', 'adventure', 'affirmative', 'affluent', 'agree', 'agreeable',
                 'amazing', 'angelic', 'appealing', 'approve', 'aptitude', 'attractive', 'awesome', 'beaming',
                 'beautiful', 'believe', 'beneficial', 'bliss', 'bountiful', 'bounty', 'brave', 'bravo', 'brilliant',
                 'bubbly', 'calm', 'celebrated', 'certain', 'champ', 'champion', 'charming', 'cheery', 'choice',
                 'classic', 'classical', 'clean', 'commend', 'composed', 'congratulation', 'constant', 'cool',
                 'courageous', 'creative', 'cute', 'dazzling', 'delight', 'delightful', 'distinguished', 'divine',
                 'earnest', 'easy', 'ecstatic', 'effective', 'effervescent', 'efficient', 'effortless', 'electrifying',
                 'elegant', 'enchanting', 'encouraging', 'endorsed', 'energetic', 'energized', 'engaging',
                 'enthusiastic', 'essential', 'esteemed', 'ethical', 'excellent', 'exciting', 'exquisite', 'fabulous',
                 'fair', 'familiar', 'famous', 'fantastic', 'favorable', 'fetching', 'fine', 'fitting', 'flourishing',
                 'fortunate', 'free', 'fresh', 'friendly', 'fun', 'funny', 'generous', 'genius', 'genuine', 'giving',
                 'glamorous', 'glowing', 'good', 'gorgeous', 'graceful', 'great', 'green', 'grin', 'growing',
                 'handsome', 'happy', 'harmonious', 'healing', 'healthy', 'hearty', 'heavenly', 'honest', 'honorable',
                 'honored', 'hug', 'idea', 'ideal', 'imaginative', 'imagine', 'impressive', 'independent', 'innovate',
                 'innovative', 'instant', 'instantaneous', 'instinctive', 'intellectual', 'intelligent', 'intuitive',
                 'inventive', 'jovial', 'joy', 'jubilant', 'keen', 'kind', 'knowing', 'knowledgeable', 'laugh',
                 'learned', 'legendary', 'light', 'lively', 'lovely', 'lucid', 'lucky', 'luminous', 'marvelous',
                 'masterful', 'meaningful', 'merit', 'meritorious', 'miraculous', 'motivating', 'moving', 'natural',
                 'nice', 'novel', 'now', 'nurturing', 'nutritious', 'okay', 'one', 'one-hundred percent', 'open',
                 'optimistic', 'paradise', 'perfect', 'phenomenal', 'pleasant', 'pleasurable', 'plentiful', 'poised',
                 'polished', 'popular', 'positive', 'powerful', 'prepared', 'pretty', 'principled', 'productive',
                 'progress', 'prominent', 'protected', 'proud', 'quality', 'quick', 'quiet', 'ready', 'reassuring',
                 'refined', 'refreshing', 'rejoice', 'reliable', 'remarkable', 'resounding', 'respected', 'restored',
                 'reward', 'rewarding', 'right', 'robust', 'safe', 'satisfactory', 'secure', 'seemly', 'simple',
                 'skilled', 'skillful', 'smile', 'soulful', 'sparkling', 'special', 'spirited', 'spiritual', 'stirring',
                 'stunning', 'stupendous', 'success', 'successful', 'sunny', 'super', 'superb', 'supporting',
                 'surprising', 'terrific', 'thorough', 'thrilling', 'thriving', 'tops', 'tranquil', 'transformative',
                 'transforming', 'trusting', 'truthful', 'unreal', 'unwavering', 'up', 'upbeat', 'upright',
                 'upstanding', 'valued', 'vibrant', 'victorious', 'victory', 'vigorous', 'virtuous', 'vital',
                 'vivacious', 'wealthy', 'welcome', 'well', 'whole', 'wholesome', 'willing', 'wonderful', 'wondrous',
                 'worthy', 'wow', 'yes', 'yummy', 'zeal', 'zealous']
negativeWords = ['abysmal', 'adverse', 'alarming', 'angry', 'annoy', 'anxious', 'apathy', 'appalling', 'atrocious',
                 'awful', 'bad', 'banal', 'barbed', 'belligerent', 'bemoan', 'beneath', 'boring', 'broken', 'callous',
                 'can\'t', 'clumsy', 'coarse', 'cold', 'cold-hearted', 'collapse', 'confused', 'contradictory',
                 'contrary', 'corrosive', 'corrupt', 'crazy', 'creepy', 'criminal', 'cruel', 'cry', 'cutting', 'damage',
                 'damaging', 'dastardly', 'dead', 'decaying', 'deformed', 'deny', 'deplorable', 'depressed', 'deprived',
                 'despicable', 'detrimental', 'dirty', 'disease', 'disgusting', 'disheveled', 'dishonest',
                 'dishonorable', 'dismal', 'distress', 'don\'t', 'dreadful', 'dreary', 'enraged', 'eroding', 'evil',
                 'fail', 'faulty', 'fear', 'feeble', 'fight', 'filthy', 'foul', 'frighten', 'frightful', 'gawky',
                 'ghastly', 'grave', 'greed', 'grim', 'grimace', 'gross', 'grotesque', 'gruesome', 'guilty', 'haggard',
                 'hard', 'hard-hearted', 'harmful', 'hate', 'hideous', 'homely', 'horrendous', 'horrible', 'hostile',
                 'hurt', 'hurtful', 'icky', 'ignorant', 'ignore', 'ill', 'immature', 'imperfect', 'impossible', 'inane',
                 'inelegant', 'infernal', 'injure', 'injurious', 'insane', 'insidious', 'insipid', 'jealous', 'junky',
                 'lose', 'lousy', 'lumpy', 'malicious', 'mean', 'menacing', 'messy', 'misshapen', 'missing',
                 'misunderstood', 'moan', 'moldy', 'monstrous', 'naive', 'nasty', 'naughty', 'negate', 'negative',
                 'never', 'no', 'nobody', 'nondescript', 'nonsense', 'not', 'noxious', 'objectionable', 'odious',
                 'offensive', 'old', 'oppressive', 'pain', 'perturb', 'pessimistic', 'petty', 'plain', 'poisonous',
                 'poor', 'prejudice', 'questionable', 'quirky', 'quit', 'reject', 'renege', 'repellant', 'reptilian',
                 'repugnant', 'repulsive', 'revenge', 'revolting', 'rocky', 'rotten', 'rude', 'ruthless', 'sad',
                 'savage', 'scare', 'scary', 'scream', 'severe', 'shocking', 'shoddy', 'sick', 'sickening', 'sinister',
                 'slimy', 'smelly', 'sobbing', 'sorry', 'spiteful', 'sticky', 'stinky', 'stormy', 'stressful', 'stuck',
                 'stupid', 'substandard', 'suspect', 'suspicious', 'tense', 'terrible', 'terrifying', 'threatening',
                 'ugly', 'undermine', 'unfair', 'unfavorable', 'unhappy', 'unhealthy', 'unjust', 'unlucky',
                 'unpleasant', 'unsatisfactory', 'unsightly', 'untoward', 'unwanted', 'unwelcome', 'unwholesome',
                 'unwieldy', 'unwise', 'upset', 'vice', 'vicious', 'vile', 'villainous', 'vindictive', 'wary', 'weary',
                 'wicked', 'woeful', 'worthless', 'wound', 'yell', 'yucky', 'zero']

# Create SparkSession and configure it
spark = SparkSession.builder.appName('streamTest') \
    .config('spark.master', 'spark://spark-master:7077') \
    .config('spark.executor.cores', 1) \
    .config('spark.cores.max', 1) \
    .config('spark.executor.memory', '1g') \
    .config('spark.sql.streaming.checkpointLocation', 'hdfs://namenode:9000/stream-checkpoint/') \
    .getOrCreate()

# Create a read stream from Kafka and a topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sentences") \
    .load()

# Cast to string
sentences = df.selectExpr("CAST(value AS STRING)")


# TODO!
# Count the sentiment of each word
def iswordpositive(msg):
    for k in positiveWords:
        if k == msg:
            return 1
    for y in negativeWords:
        if y == msg:
            return -1
    return 0


sentiment = []
for x in sentences:
    sentiment[x] = iswordpositive(x)

# Write it back to Kafka
columns = [col('word'), col('count')]
mergedColumns = (sentiment, array(columns))

mergedColumns.select(to_json(mergedColumns.value).alias('value')).selectExpr("CAST(value AS STRING)").writeStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "word-counts") \
    .outputMode("complete") \
    .start().awaitTermination()