package cn.tool

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * @Auther: juanjie
  * @Date: 2019/3/20 20:35
  * @Description:
  */
object JedisPoolUtils{
  private final val config = new JedisPoolConfig()
  //最大空闲连接的数量
  config.setMaxIdle(5)
  //连接池中最大的连接数量
  config.setMaxIdle(2000)
  //创建一个jedis连接池
  private final val pool = new JedisPool(config,"hdp03")

  //定义一个方法，返回一个Jedis连接
  def getJedis()={
   pool.getResource()
  }

}
