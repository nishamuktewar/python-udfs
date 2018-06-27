conda create -n py2_env --copy -y -q python=3 statsmodels pyarrow
cd ~/.conda/envs
zip -r ../../py3_env.zip py3_env

spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3/py3_env/bin/python
spark.yarn.dist.archives=py3_env.zip#PY3