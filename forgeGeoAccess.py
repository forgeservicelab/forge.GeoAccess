"""Spark job to compute the accesses to FORGE services, grouped by country."""

from pyspark import SparkContext, SparkConf, SparkFiles, HiveContext
from pyspark.sql.types import *
from operator import add
import geoip2.database as geoIP
import itertools
import sys


def _getCountryByIP(ip):
    citydb = geoIP.Reader(SparkFiles.get('GeoLite2-City.mmdb'))
    return (citydb.city(ip).country.name or u'Unknown').encode()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: forgeInternationalAccess <date> <hour>"
        exit(-1)

    spark = SparkContext(appName='ForgeGeoAccess')
    spark.addPyFile('hdfs://digiledap/user/spark/share/lib/accessLogParser.py')
    spark.addFile('hdfs://digiledap/user/spark/share/lib/GeoLite2-City.mmdb')

    from accessLogParser import *
    from snakebite.client import Client

    hdfsHandle = Client('hmaster01')
    hosts = spark.parallelize(hdfsHandle.ls(['/flume/events/apache_access_combined/']))\
                 .filter(lambda dirs: dirs['file_type'] == 'd')\
                 .map(lambda directory: 'hdfs://digiledap%s' % directory['path'])\
                 .collect()

    rdds = {
        item.split('/')[-1]: spark.textFile('%s/%s/%s' % (item, sys.argv[1], sys.argv[2])) for item in hosts
    }

    results = {
        key: rdds[key].map(lambda log: Parser.create(Parser.COMBINED).parse(log))
                      .map(lambda log: (((log['timestamp'] - timedelta(minutes=log['timestamp'].minute % 5))
                                         .replace(second=0),
                                         _getCountryByIP(log['remote_ip'].compressed)),
                                        1))
                      .reduceByKey(add).map(lambda x: (key, x[0][0], x[0][1], x[1])) for key in rdds
    }

    host = 'hmaster02'
    table = 'hbase_test'
    conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

    spark.parallelize(['row2', 'cf', 'b', 'value2']).map(lambda x: (x[0], x)).saveAsNewAPIHadoopDataset(
        conf=conf,
        keyConverter=keyConv,
        valueConverter=valueConv)

    spark.stop()
