conda create -n py2_env --copy -y -q python=3 statsmodels pyarrow
cd ~/.conda/envs
zip -r ../../py3_env.zip py3_env

# This is for spark-defaults.conf
spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py3_env/bin/python
spark.yarn.dist.archives=py3_env.zip#PY3

# Then add the PYSPARK_PYTHON environment variable to the project, under "settings"
PYSPARK ./PY3/py3_env/bin/python