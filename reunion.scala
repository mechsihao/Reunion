import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.Random.nextInt


object Reunion {

  class aggFunHelper extends java.io.Serializable{
    var firstAggFunc = ""
    var secondAggFunc = ""

    val firstSecondFuncMap: Map[String, String] = Map(
      "sum" -> "sum",
      "max" -> "max",
      "min" -> "min",
      "avg" -> "avg_trans_fun",
      "count" -> "count",
      "collect_set" -> "collect_set_trans_fun",
      "collect_list" -> "collect_list_trans_fun"
    )

    val firstFunc: Map[String, String => String] = Map(
      "sum" -> ((x: String) => s"sum($x) as $x"),
      "max" -> ((x: String) => s"max($x) as $x"),
      "min" -> ((x: String) => s"min($x) as $x"),
      "avg" -> ((x: String) => s"concat_ws(',', collect_list(cast($x as string))) as $x"),
      "count" -> ((x: String) => s"count($x) as $x"),
      "collect_set" -> ((x: String) => s"collect_set($x) as $x"),
      "collect_list" -> ((x: String) => s"collect_list($x) as $x")
    )

    val secondFunc: Map[String, String => String] = Map(
      "sum" -> ((x: String) => s"sum($x) as $x"),
      "max" -> ((x: String) => s"max($x) as $x"),
      "min" -> ((x: String) => s"min($x) as $x"),
      "avg" -> ((x: String) => s"${firstSecondFuncMap("avg")}(concat_ws(',', collect_list($x))) as $x"),
      "count" -> ((x: String) => s"sum($x) as $x"),
      "collect_set" -> ((x: String) => s"${firstSecondFuncMap("collect_set")}(collect_set($x)) as $x"),
      "collect_list" -> ((x: String) => s"${firstSecondFuncMap("collect_list")}(collect_list($x)) as $x")
    )

    def this(inputFirstAggFun: String, spark: SparkSession){
      this()
      this.firstAggFunc = inputFirstAggFun
      this.secondAggFunc = firstSecondFuncMap(inputFirstAggFun)
      this.initUdf(spark)
    }

    def initUdf(spark: SparkSession): Unit = {
      if (this.firstAggFunc == "avg") {
        spark.udf.register(this.secondAggFunc, this.avgTransFunc _)
      }else if (this.firstAggFunc == "collect_set") {
        spark.udf.register(this.secondAggFunc, this.collectSetTransFunc _)
      }else if (this.firstAggFunc == "collect_list") {
        spark.udf.register(this.secondAggFunc, this.collectListTransFunc _)
      }
    }

    def firstExpr(colName: String): String = {
      this.firstFunc(this.firstAggFunc)(colName)
    }

    def secondExpr(colName: String): String = {
      this.secondFunc(this.firstAggFunc)(colName)
    }

    def avgTransFunc(numStr: String): Double = {
      try{
        val ret = numStr.split(",").map(_.toDouble)
        ret.sum / ret.length
      }catch{
        case e: Throwable =>
          println(s"平均数分割计算UDF错误，numStr='$numStr'，详细日志：${e.toString}")
          0.0
      }
    }

    def collectSetTransFunc(arr: mutable.WrappedArray[mutable.WrappedArray[String]]):mutable.WrappedArray[String] = {
      try{
        arr.flatMap(_.toList)
      }catch{
        case e: Throwable =>
          println(s"Collect Set重聚合UDF错误，numStr='${arr.toString}'，详细日志：${e.toString}")
          Array[String]()
      }
    }

    def collectListTransFunc(arr: mutable.WrappedArray[mutable.WrappedArray[String]]): mutable.WrappedArray[String] = {
      try{
        arr.flatMap(_.toList)
      }catch{
        case e: Throwable =>
          println(s"Collect List重聚合UDF错误，numStr='${arr.toString}'，详细日志：${e.toString}")
          Array[String]()
      }
    }

  }

  def matchFuncCols(inputStr: String, supportAggFun: Array[String]): (String, String, String, String, String) = {
    // 返回的 格式 name fun rename concat_ws sep
    if (inputStr.contains("group by")){
      val name = inputStr.split("group by")(1).stripPrefix(" ").stripSuffix(" ")
      val nameStrList = name.split(" as ")
      if (nameStrList.length == 1){
        val name = nameStrList(0).stripPrefix(" ").stripSuffix(" ")
        if (name.contains("if") || (name.contains("(") && name.contains(")") || (name.contains("case") && name.contains("when"))) ) {
          throw new Throwable("当主键包含函数时必须用as给主键起别名, 如: if(a, a, b) as a_or_b")
        }
        (name, "", name, "", "")
      }else{
        val name = nameStrList(0).stripPrefix(" ").stripSuffix(" ")
        val rename = nameStrList(1).stripPrefix(" ").stripSuffix(" ")
        (name, "", rename, "", "")
      }
    }else{
      val nameStrList = inputStr.split(" as ")
      val (nameStr, renameStr) =  if (nameStrList.length == 1){
        val name = nameStrList(0).stripPrefix(" ").stripSuffix(" ")
        if (name.contains("count(*)")) {
          throw new Throwable("当聚合方法为count(*)时需要给新生成的列起别名, 如: count(*) as nums")
        }else{
          (name, "")
        }
      }else{
        val name = nameStrList(0).stripPrefix(" ").stripSuffix(" ")
        val rename = nameStrList(1).stripPrefix(" ").stripSuffix(" ")
        (name, rename)
      }
      def matchF(funAndName: String): (String, String) = {
        val ret = supportAggFun.map(
          func=> {
            val regex = ".*%s\\((.*)\\)*.".format(func).r
            val orgName = try {
              val regex(str) = funAndName
              str
            }catch{
              case _: Exception => ""
            }
            (orgName, func)
          }
        ).filter(_._1 != "")
        if (ret.isEmpty){
          throw new Exception(s"无法识别$funAndName")
        }else{
          ret(0)
        }
      }

      val ((orgNameStr, func), (concatFunc, sep)) = if (nameStr.contains("concat_ws")){
        val regex = s""".*concat_ws\\((.*)\\)*.""".r
        val regex(str) = nameStr
        val sep = s"""${str.split("'")(1)}"""
        val funNameStr = str.split("'")(2).split(",")(1).stripPrefix(" ").stripSuffix(" ")
        (matchF(funNameStr), ("concat_ws", sep))
      }else{
        (matchF(nameStr), ("", ""))
      }
      // 将未重命名的列 命名成 原列名
      if (renameStr == ""){
        (orgNameStr, func, orgNameStr, concatFunc, sep)
      }else{
        (orgNameStr, func, renameStr, concatFunc, sep)
      }
    }
  }

  def colMaker(infos: (String, String, String, String, String), useReunion: Boolean=false, unionStat: Int=1)(implicit spark: SparkSession): String = {

    def colMakerNotReunion(infos: (String, String, String, String, String)): String = {
      if (infos._4 != "")
        s"concat_ws('${infos._5}', ${infos._2}(${infos._3})) as ${infos._3}"
      else{
        if (infos._1 == "*" && infos._2 == "count")
          s"count(*) as ${infos._3}"
        else if (infos._1.contains("distinct") && infos._2 == "count") {
          if (infos._1.contains("*"))
            s"count(distinct *) as ${infos._3}"
          else
            s"count(distinct ${infos._3}) as ${infos._3}"
        } else if (infos._2 != "")
          s"${infos._2}(${infos._3}) as ${infos._3}"
        else
          s"${infos._3}"
      }
    }

    def colMakerReunion(infos: (String, String, String, String, String), unionStat: Int): String = {
      if (infos._1 == "*" && infos._2 == "count"){
        // 这个代表是 * 的 聚合建
        if (unionStat == 1)
          s"count(*) as ${infos._3}"
        else if (unionStat == 2)
          s"sum(${infos._3}) as ${infos._3}"
        else {
          s"${infos._3}"
        }
      } else if (infos._1.contains("distinct") && infos._2 == "count") {
        // 这个代表是 distinct xxx 的 聚合建
        if (unionStat == 1 || unionStat ==2){
          if (infos._1.contains("*"))
            s"count(distinct *) as ${infos._3}"
          else
            s"count(distinct ${infos._3}) as ${infos._3}"
        } else {
          s"${infos._3}"
        }
      } else if (infos._2 != ""){
        // 这个代表是 主键
        val helper = new aggFunHelper(infos._2, spark)
        if (unionStat == 1)
          helper.firstExpr(infos._3)
        else if (unionStat == 2)
          helper.secondExpr(infos._3)
        else
          // 这层代表有concat_ws
          if (infos._4 != "")
            s"concat_ws('${infos._5}', ${infos._3}) as ${infos._3}"
          else
            s"${infos._3}"
      } else {
        s"${infos._3}"
      }
    }

    if (useReunion)
      colMakerReunion(infos, unionStat)
    else
      colMakerNotReunion(infos)
  }

  def appendRandomInt(key: Any, aggRandomFrac: Int): String = {
    try{
      val tmpKey = if (key == null){
        ""
      }else{
        key.toString
      }
      s"${tmpKey}_${nextInt(aggRandomFrac)}"
    }catch{
      case e: Throwable =>
        println(s"增加随机数失败,key='$key'，详细日志: ${e.toString}")
        ""
    }
  }

  def splitRandomInt(key: Any): String = {
    try {
      val tmpKeys = key.toString.split("_")
      tmpKeys.slice(0, tmpKeys.length - 1).mkString("_")
    } catch {
      case e: Throwable =>
        println(s"切分随机数失败，key='$key'，详细日志: ${e.toString}")
        ""
    }
  }

  def printHql(hql: String): Unit = {
    val len = hql.split("\n").map(_.length).max
    println("╭" + "─" * len + "╮")
    val pHql = hql.trim.split("\n")
    pHql.map(_.trim).foreach(x => println("│" + x + " " * (len - x.length)  + "│"))
    println("╰" + "─" * len + "╯")
  }

  def printKeysMapTable(keyMap: Array[(String, String, String, String, String)]): Unit = {
    val maxLen = 41

    def getLinePrintStr(Arr: (String, String, String, String, String), lenArr: Array[Int], prefix: String, sep: String, suffix: String, side: Boolean = false): String = {
      if (side){
        Arr.productIterator.zipWithIndex.map {
          case (x, i) =>
            val str = x.toString
            val len = lenArr(i)
            str * (len - 1)
        }.mkString(prefix, sep, suffix)
      } else {
        Arr.productIterator.zipWithIndex.map {
          case (x, i) =>
            val str = x.toString
            val len = lenArr(i)
            " " * ((len - str.length) / 2) + x + " " * ((len - str.length) / 2)
        }.mkString(prefix, sep, suffix)
      }
    }

    val keyMapTrans = mutable.ArrayBuffer[(String, String, String, String, String)]()
    keyMapTrans += (("ORIGIN_COL", "FUNCTION", "RENAME_COL", "USE_CONCAT", "CONCAT_SEP"))

    keyMap.map(x => {
        val orgName = if (x._1.contains("\n")) x._1.split("\n").mkString(" ") else x._1
        val func = if (x._2.contains("\n")) x._2.split("\n").mkString(" ") else x._2
        val reName = if (x._3.contains("\n")) x._3.split("\n").mkString(" ") else x._3
        val useConcat = if (x._4.contains("\n")) x._4.split("\n").mkString(" ") else x._4
        val concatSep = if (x._5.contains("\n")) x._5.split("\n").mkString(" ") else x._5
        Array(orgName, func, reName, useConcat, concatSep)
      }).foreach(x => {
      val orgName = if (x(0).length > maxLen) x(0).substring(0, maxLen) + "..." else if (x(0).length % 2 == 0) x(0) else x(0) + " "
      val func = if (x(1).length > maxLen) x(1).substring(0, maxLen) + "..." else if (x(1).isEmpty) "[GROUP BY KEY]" else if (x(1).length % 2 == 0) x(1) else x(1) + " "
      val reName = if (x(2).length > maxLen) x(2).substring(0, maxLen) + "..." else if (x(2).length % 2 == 0) x(2) else x(2) + " "
      val useConcat = if (x(3).nonEmpty) "TRUE" else "FALSE "
      val concatSep = if (x(4).length > maxLen) x(4).substring(0, maxLen) + "..." else if (x(4).isEmpty) "  " else if (x(4).length % 2 == 0) s"'${x(4)}'" else s"'${x(4)}' "
      keyMapTrans += ((orgName, func, reName, useConcat, concatSep))
    })

    val lenArr = keyMapTrans.map(
      x => Array(x._1.length + 3, x._2.length + 3, x._3.length + 3, x._4.length + 3, x._5.length + 3)
    ).transpose.map(_.max).toArray

    println("Key映射信息为：")
    keyMapTrans.zipWithIndex.foreach {
      case (x, i) =>
        if (i == 0){
          println(getLinePrintStr(("─", "─", "─", "─", "─"), lenArr, "╭", "┬", "╮", side = true))
        }
        println(getLinePrintStr(x, lenArr, "│", "│", "│"))
        if (i == 0){
          println(getLinePrintStr(("─", "─", "─", "─", "─"), lenArr, "├", "┼", "┤", side = true))
        }
        if (i == keyMapTrans.length - 1) {
          println(getLinePrintStr(("─", "─", "─", "─", "─"), lenArr, "╰", "┴", "╯", side = true))
        }
    }

  }

  /** 从表中 select group by 的过程，该过程的主要价值在于实现 多次聚合 来减轻数据倾斜问题的方法
   *
   * 目前支持的聚合函数为：("avg", "sum", "count", "max", "min", "collect_list", "collect_set")
   *
   * @author 李思浩 (80302421)
   * @param inputDf DataFrame 输入数据[DataFrame]
   * @param table String 输入数据[表名]，inputDf和table只能输入一个，table可以是hive表，也可以是全局临时表，即.createGlobalTempView()得到的表
   * @param whereStr String 条件语句
   * @param selectColsFun Array[String] 需要字段及其聚合规则
   *                              - 1.对于聚合主键的，需要在前面加上group by，例如group by name
   *                              - 2.对于待聚合的键，则用相应的聚合函数括起要处理的列例如 collect_set(company)
   *                              - 3.支持重命名 需要加上as字段，例如 group by name as people; collect_set(company) as company_list，如果不重命名，则会使用原来的列名
   *                              - 4.支持对 concat 结合 collect_set/collect_list 一起使用，例如 concat_ws(',', collect_set(match_type)) as match_types，如果想要使用制表符则分隔符为\\t
   * @param useReAggregation Boolean 是否使用重聚合，默认true
   * @param aggRandomFrac Int 重聚合的随机种子范围，默认1000
   * @param orderByKeys Array[String] 按照那些列排序，列名为重命名的列名
   * @param ascending 是否顺序 默认true
   * @param showLog Boolean 是否输出SQL语句，默认false
   * @return DataFrame 返回 DF 的列为 selectColsFun 中的所有 rename 后的列
   *
   * @example 示例1，输入DataFrame：
   * {{{
   * // 先定义一个 df
   * val inputDf = Seq(
   *   ("a", 1.2, 111), ("a", 0.3, 222), ("a", 2.3, 111), ("a", 0.3, 222), ("b", 1.6, 222), ("c", 9.3, 111), ("c", 9.4, 222)
   * ).toDF("kw", "price", "code")
   *
   * inputDf.show()
   * // 这个df长这样：
   * +---+-----+----+
   * | kw|price|code|
   * +---+-----+----+
   * |  a|  1.2| 111|
   * |  a|  0.3| 222|
   * |  a|  2.3| 111|
   * |  a|  0.3| 222|
   * |  b|  1.6| 222|
   * |  c|  9.3| 111|
   * |  c|  9.4| 222|
   * +---+-----+----+
   *
   * // 现在对其进行重聚合
   * groupByReunion(
   *   inputDf = inputDf,
   *   selectColsFun = Array(
   *     "group by kw",
   *     "sum(price) as income",
   *     "count(*) as qv",
   *     "count(distinct *) as uqv",
   *     "count(distinct code) as uv"
   *   ),
   *   useReAggregation = true,
   *   orderByKeys = Array("qv", "search_key"),
   *   ascending = false
   * ).show()
   *
   * // output:
   * +----+------+----+----+----+
   * |  kw|income|  qv| uqv|  uv|
   * +----+------+----+----+----+
   * |   a|   3.8|   4|   4|   2|
   * |   b|   1.6|   1|   1|   1|
   * |   c|   9.3|   2|   2|   2|
   * +----+------+----+----+----+
   * }}}
   *
   * @example 示例2，输入hive表：
   * {{{
   * groupByReunion(
   *   table = "ad_tag.f_tag_dict_recall",
   *   whereStr = "dayno=20220628 and key_type='query'",
   *   selectColsFun = Array(
   *     "group by search_key",
   *     "count(*) as qv",
   *     "concat_ws(',', collect_set(match_type)) as match_types"
   *   ),
   *   useReAggregation = true,
   *   orderByKeys = Array("qv", "search_key"),
   *   ascending = false
   * ).filter("qv > 10").show(3)
   *
   * output:
   * +-----------+----+-----------------+
   * | search_key|  qv|      match_types|
   * +-----------+----+-----------------+
   * |      小鲨鱼|   8|   1000,2000,4000|
   * |      小兔子|   3|        2000,8000|
   * |      要贴贴|   3|        2000,8000|
   * |      是密接|   2|             2000|
   * +-----------+----+-----------------+
   * }}}
   *
   * @example 示例3，复杂逻辑聚合：
   * {{{
   * val startDayno = "20220704"
   * val dayno = "20220711"
   * val dfBsExp = groupByReunion(
   *   table = "ad.f_ads_bs_js_exp",
   *   selectColsFun = Array(
   *    "group by if(trim(rkw)='', trim(kw), trim(rkw)) as keyword",
   *    "group by adid as ad_id",
   *    """group by
   *       |case
   *       |when coalesce(trim(sc), '') in ('3', '') then 'browse_search'
   *       |when sc in ('10') then 'download_card'
   *       |when sc in ('4', '5', '6', '22') then 'global_search'
   *       |when sc in ('25') then 'lock_screen'
   *       |else 'other' end as sc
   *       |""".stripMargin,  // group by 到 as sc 中间的部分逻辑是作用于Hive表原表的，因此只要合法即可随意写
   *    "count(*) as exp_nums"
   *   ),
   *   aggRandomFrac=100000,
   *   whereStr=s"dayno > $startDayno and dayno <= $dayno and coalesce(trim(flag), '') = '' and coalesce(trim(sc),'') in ('','3','4','5','6','10','22','25')",
   *   showLog=true
   * )
   *
   * output:
   * +-----------+-------------+---------+
   * |    keyword|           sc| exp_nums|
   * +-----------+-------------+---------+
   * |     三国演义|download_card|   134500|
   * |     英雄联盟|global_search|     2023|
   * |     抖音视频|  lock_screen|   324456|
   * +-----------+-------------+---------+
   * }}}
   */
  def groupByReunion(
                      inputDf: DataFrame = null,
                      table: String = null,
                      whereStr:String = "",
                      selectColsFun: Array[String],
                      useReAggregation: Boolean = true,
                      aggRandomFrac: Int = 1000,
                      orderByKeys: Array[String] = Array(),
                      ascending: Boolean = true,
                      showLog: Boolean = false
                    )(implicit spark: SparkSession): DataFrame = {
    // 校验逻辑
    assert(!(inputDf == null && table == null), "请输入一个df或者一个table")
    assert(!(inputDf != null && table != null), "只允许输入一种数据类型")
    val supportAggFun = Array("avg", "sum", "count", "max", "min", "collect_list", "collect_set")
    val tmpTableName = "tmp_table"

    if (inputDf == null && !spark.catalog.tableExists(table))
      throw new Throwable(s"Table不存在：'$table'")
    else if (showLog)
      println(s"Table已校验存在：'$table'")

    val keyMap = selectColsFun.map(matchFuncCols(_, supportAggFun))
    if (showLog)
      printKeysMapTable(keyMap)

    val starRenameKey = keyMap.filter(_._1.contains("*")).map(_._3)
    assert(keyMap.map(_._3).toSet.size == keyMap.length, "输出数据的列不可重名")

    val org_df = if (inputDf != null) {
      if (whereStr.isEmpty)
        inputDf
      else
        inputDf.filter(whereStr)
    } else
      spark.sql(s"select * from $table ${if (whereStr.isEmpty) "" else "where"} $whereStr")

    val df = org_df.selectExpr(keyMap.filter(!_._1.contains("*")).map(
        x => if (x._1.contains("distinct")) {
          s"${x._1.split(" ")(1)} as ${x._3}"
        } else {
          s"${x._1} as ${x._3}"
        }): _*)

    df.createOrReplaceTempView(tmpTableName)

    orderByKeys.foreach(
      key =>
        if (!df.columns.contains(key) && !starRenameKey.contains(key))
          throw new Exception(s"输入和聚合后均不包含该列:'$key'，列名为: [${df.columns.mkString(",")}]")
    )

    val firstAllSelectKeys = keyMap.filter(!_._1.contains("*")).map(_._3)
    val secondAllSelectKeys = keyMap.map(_._3)
    val primaryKeys = keyMap.filter(_._2 == "").map(_._3)  // group by 的 keys

    if (!useReAggregation) {
      // 不使用重聚合，则直接 group by 即可
      val sqlStr = s"""
      |select ${keyMap.map(colMaker(_)).mkString("\n, ")}
      |from
      |$tmpTableName
      |group by ${primaryKeys.mkString(", ")}
      |${if (orderByKeys.isEmpty) "" else "order by"} ${orderByKeys.mkString(", ")} ${if (ascending || orderByKeys.isEmpty) "" else "desc"}
      |""".stripMargin

      if (showLog){
        println("不使用重聚合，执行sql：")
        printHql(sqlStr)
      }

      spark.sql(sqlStr)
    }else{
      // 使用重聚合，先将 待group by的key 拼接上一个随机数
      spark.udf.register("append_random_int", (x: String) => appendRandomInt(x, aggRandomFrac))
      df.selectExpr(firstAllSelectKeys.map(x => if (primaryKeys.contains(x)) s"append_random_int($x) as $x" else x): _*).createOrReplaceTempView(tmpTableName + "1")

      val sqlStr1 = s"""
      |select ${keyMap.map(colMaker(_, useReunion = true)).mkString("\n, ")}
      |from
      |${tmpTableName + "1"}
      |group by ${primaryKeys.mkString(", ")}
      |""".stripMargin

      if (showLog) {
        println("使用重聚合，第一阶段执行sql：")
        printHql(sqlStr1)
      }

      val tmpDf2 = spark.sql(sqlStr1)
      spark.udf.register("split_random_int", splitRandomInt _)
      tmpDf2.selectExpr(secondAllSelectKeys.map(x => if (primaryKeys.contains(x)) s"split_random_int($x) as $x" else x): _*).createOrReplaceTempView(tmpTableName + "2")

      val sqlStr2 = s"""
      |select ${keyMap.map(colMaker(_, useReunion = true, unionStat = 2)).mkString("\n, ")}
      |from
      |${tmpTableName + "2"}
      |group by ${primaryKeys.mkString(", ")}
      |${if (orderByKeys.isEmpty) "" else "order by"} ${orderByKeys.mkString(", ")} ${if (ascending || orderByKeys.isEmpty) "" else "desc"}
      |""".stripMargin

      if (showLog) {
        println("使用重聚合，第二阶段执行sql：")
        printHql(sqlStr2)
      }

      val resDf = spark.sql(sqlStr2)

      if (keyMap.exists(_._4 != "")){
        resDf.createOrReplaceTempView(tmpTableName + "3")
        spark.sql(
          s"""
             |select ${keyMap.map(colMaker(_, useReunion = true, unionStat = 3)).mkString(", ")}
             |from
             |${tmpTableName + "3"}
             |${if (orderByKeys.isEmpty) "" else "order by"} ${orderByKeys.mkString(", ")} ${if (ascending) "" else "desc"}
             |""".stripMargin
        )
      } else {
        resDf
      }
    }
  }

}
