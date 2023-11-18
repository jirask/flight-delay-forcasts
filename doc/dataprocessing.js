Index.PACKAGES = {"com.preprocess.weather" : [{"name" : "com.preprocess.weather.Weather", "shortDescription" : "", "members_case class" : [{"member" : "com.preprocess.weather.Weather#<init>", "error" : "unsupported entity"}, {"label" : "HourlyPrecip", "tail" : ": Double", "member" : "com.preprocess.weather.Weather.HourlyPrecip", "link" : "com\/preprocess\/weather\/Weather.html#HourlyPrecip:Double", "kind" : "val"}, {"label" : "WeatherType", "tail" : ": String", "member" : "com.preprocess.weather.Weather.WeatherType", "link" : "com\/preprocess\/weather\/Weather.html#WeatherType:String", "kind" : "val"}, {"label" : "WindSpeed", "tail" : ": Double", "member" : "com.preprocess.weather.Weather.WindSpeed", "link" : "com\/preprocess\/weather\/Weather.html#WindSpeed:Double", "kind" : "val"}, {"label" : "Visibility", "tail" : ": Double", "member" : "com.preprocess.weather.Weather.Visibility", "link" : "com\/preprocess\/weather\/Weather.html#Visibility:Double", "kind" : "val"}, {"label" : "SkyCOndition", "tail" : ": String", "member" : "com.preprocess.weather.Weather.SkyCOndition", "link" : "com\/preprocess\/weather\/Weather.html#SkyCOndition:String", "kind" : "val"}, {"label" : "DryBulbCelsius", "tail" : ": Double", "member" : "com.preprocess.weather.Weather.DryBulbCelsius", "link" : "com\/preprocess\/weather\/Weather.html#DryBulbCelsius:Double", "kind" : "val"}, {"label" : "Weather_TIMESTAMP", "tail" : ": Timestamp", "member" : "com.preprocess.weather.Weather.Weather_TIMESTAMP", "link" : "com\/preprocess\/weather\/Weather.html#Weather_TIMESTAMP:java.sql.Timestamp", "kind" : "val"}, {"label" : "AIRPORT_ID", "tail" : ": Int", "member" : "com.preprocess.weather.Weather.AIRPORT_ID", "link" : "com\/preprocess\/weather\/Weather.html#AIRPORT_ID:Int", "kind" : "val"}, {"label" : "synchronized", "tail" : "(arg0: ⇒ T0): T0", "member" : "scala.AnyRef.synchronized", "link" : "com\/preprocess\/weather\/Weather.html#synchronized[T0](x$1:=>T0):T0", "kind" : "final def"}, {"label" : "##", "tail" : "(): Int", "member" : "scala.AnyRef.##", "link" : "com\/preprocess\/weather\/Weather.html###():Int", "kind" : "final def"}, {"label" : "!=", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.!=", "link" : "com\/preprocess\/weather\/Weather.html#!=(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "==", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.==", "link" : "com\/preprocess\/weather\/Weather.html#==(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "ne", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.ne", "link" : "com\/preprocess\/weather\/Weather.html#ne(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "eq", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.eq", "link" : "com\/preprocess\/weather\/Weather.html#eq(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "finalize", "tail" : "(): Unit", "member" : "scala.AnyRef.finalize", "link" : "com\/preprocess\/weather\/Weather.html#finalize():Unit", "kind" : "def"}, {"label" : "wait", "tail" : "(arg0: Long, arg1: Int): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/weather\/Weather.html#wait(x$1:Long,x$2:Int):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(arg0: Long): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/weather\/Weather.html#wait(x$1:Long):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/weather\/Weather.html#wait():Unit", "kind" : "final def"}, {"label" : "notifyAll", "tail" : "(): Unit", "member" : "scala.AnyRef.notifyAll", "link" : "com\/preprocess\/weather\/Weather.html#notifyAll():Unit", "kind" : "final def"}, {"label" : "notify", "tail" : "(): Unit", "member" : "scala.AnyRef.notify", "link" : "com\/preprocess\/weather\/Weather.html#notify():Unit", "kind" : "final def"}, {"label" : "clone", "tail" : "(): AnyRef", "member" : "scala.AnyRef.clone", "link" : "com\/preprocess\/weather\/Weather.html#clone():Object", "kind" : "def"}, {"label" : "getClass", "tail" : "(): Class[_]", "member" : "scala.AnyRef.getClass", "link" : "com\/preprocess\/weather\/Weather.html#getClass():Class[_]", "kind" : "final def"}, {"label" : "asInstanceOf", "tail" : "(): T0", "member" : "scala.Any.asInstanceOf", "link" : "com\/preprocess\/weather\/Weather.html#asInstanceOf[T0]:T0", "kind" : "final def"}, {"label" : "isInstanceOf", "tail" : "(): Boolean", "member" : "scala.Any.isInstanceOf", "link" : "com\/preprocess\/weather\/Weather.html#isInstanceOf[T0]:Boolean", "kind" : "final def"}], "case class" : "com\/preprocess\/weather\/Weather.html", "kind" : "case class"}, {"name" : "com.preprocess.weather.WeatherPreprocessing", "shortDescription" : "This class processes the weather table by: cleaning NA values: switching with the average or removing them, and keeping only columns of interest", "members_class" : [{"label" : "getProcessedWeatherData", "tail" : "(): DataFrame", "member" : "com.preprocess.weather.WeatherPreprocessing.getProcessedWeatherData", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#getProcessedWeatherData:org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "buildWeatherTable", "tail" : "(): DataFrame", "member" : "com.preprocess.weather.WeatherPreprocessing.buildWeatherTable", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#buildWeatherTable():org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "extractSkyCondition", "tail" : ": UserDefinedFunction", "member" : "com.preprocess.weather.WeatherPreprocessing.extractSkyCondition", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#extractSkyCondition:org.apache.spark.sql.expressions.UserDefinedFunction", "kind" : "val"}, {"member" : "com.preprocess.weather.WeatherPreprocessing#<init>", "error" : "unsupported entity"}, {"label" : "synchronized", "tail" : "(arg0: ⇒ T0): T0", "member" : "scala.AnyRef.synchronized", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#synchronized[T0](x$1:=>T0):T0", "kind" : "final def"}, {"label" : "##", "tail" : "(): Int", "member" : "scala.AnyRef.##", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html###():Int", "kind" : "final def"}, {"label" : "!=", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.!=", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#!=(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "==", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.==", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#==(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "ne", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.ne", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#ne(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "eq", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.eq", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#eq(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "finalize", "tail" : "(): Unit", "member" : "scala.AnyRef.finalize", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#finalize():Unit", "kind" : "def"}, {"label" : "wait", "tail" : "(arg0: Long, arg1: Int): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#wait(x$1:Long,x$2:Int):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(arg0: Long): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#wait(x$1:Long):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#wait():Unit", "kind" : "final def"}, {"label" : "notifyAll", "tail" : "(): Unit", "member" : "scala.AnyRef.notifyAll", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#notifyAll():Unit", "kind" : "final def"}, {"label" : "notify", "tail" : "(): Unit", "member" : "scala.AnyRef.notify", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#notify():Unit", "kind" : "final def"}, {"label" : "toString", "tail" : "(): String", "member" : "scala.AnyRef.toString", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#toString():String", "kind" : "def"}, {"label" : "clone", "tail" : "(): AnyRef", "member" : "scala.AnyRef.clone", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#clone():Object", "kind" : "def"}, {"label" : "equals", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.equals", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#equals(x$1:Any):Boolean", "kind" : "def"}, {"label" : "hashCode", "tail" : "(): Int", "member" : "scala.AnyRef.hashCode", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#hashCode():Int", "kind" : "def"}, {"label" : "getClass", "tail" : "(): Class[_]", "member" : "scala.AnyRef.getClass", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#getClass():Class[_]", "kind" : "final def"}, {"label" : "asInstanceOf", "tail" : "(): T0", "member" : "scala.Any.asInstanceOf", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#asInstanceOf[T0]:T0", "kind" : "final def"}, {"label" : "isInstanceOf", "tail" : "(): Boolean", "member" : "scala.Any.isInstanceOf", "link" : "com\/preprocess\/weather\/WeatherPreprocessing.html#isInstanceOf[T0]:Boolean", "kind" : "final def"}], "class" : "com\/preprocess\/weather\/WeatherPreprocessing.html", "kind" : "class"}], "com.preprocess" : [], "com.join" : [{"name" : "com.join.DataJoin", "shortDescription" : "", "members_class" : [{"label" : "executePipeline", "tail" : "(): Unit", "member" : "com.join.DataJoin.executePipeline", "link" : "com\/join\/DataJoin.html#executePipeline():Unit", "kind" : "def"}, {"label" : "splitAndSaveData", "tail" : "(df: DataFrame, outputPath: String): Unit", "member" : "com.join.DataJoin.splitAndSaveData", "link" : "com\/join\/DataJoin.html#splitAndSaveData(df:org.apache.spark.sql.DataFrame,outputPath:String):Unit", "kind" : "def"}, {"label" : "createFinalTable", "tail" : "(ProcessedFlightWeatherTable: DataFrame): DataFrame", "member" : "com.join.DataJoin.createFinalTable", "link" : "com\/join\/DataJoin.html#createFinalTable(ProcessedFlightWeatherTable:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "ProcessFlightWeatherTable", "tail" : "(joinedFlightsAndWeatherFinal: DataFrame): DataFrame", "member" : "com.join.DataJoin.ProcessFlightWeatherTable", "link" : "com\/join\/DataJoin.html#ProcessFlightWeatherTable(joinedFlightsAndWeatherFinal:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "joinFlightsAndWeatherFinal", "tail" : "(destFlights: DataFrame, weatherOriginPartitions: DataFrame, numPartitions: Int): DataFrame", "member" : "com.join.DataJoin.joinFlightsAndWeatherFinal", "link" : "com\/join\/DataJoin.html#joinFlightsAndWeatherFinal(destFlights:org.apache.spark.sql.DataFrame,weatherOriginPartitions:org.apache.spark.sql.DataFrame,numPartitions:Int):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "createFlightDestination", "tail" : "(aggregatedFOT: DataFrame): DataFrame", "member" : "com.join.DataJoin.createFlightDestination", "link" : "com\/join\/DataJoin.html#createFlightDestination(aggregatedFOT:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "aggregateFOT", "tail" : "(WeatherOriginAndFlightOriginJoined: DataFrame): DataFrame", "member" : "com.join.DataJoin.aggregateFOT", "link" : "com\/join\/DataJoin.html#aggregateFOT(WeatherOriginAndFlightOriginJoined:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "joinWeatherOriginAndFlightOrigin", "tail" : "(WeatherOriginPartition: DataFrame, FlightOriginPartition: DataFrame): DataFrame", "member" : "com.join.DataJoin.joinWeatherOriginAndFlightOrigin", "link" : "com\/join\/DataJoin.html#joinWeatherOriginAndFlightOrigin(WeatherOriginPartition:org.apache.spark.sql.DataFrame,FlightOriginPartition:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "createFlightOriginPartition", "tail" : "(FlightOrigin: DataFrame, numPartitions: Int): DataFrame", "member" : "com.join.DataJoin.createFlightOriginPartition", "link" : "com\/join\/DataJoin.html#createFlightOriginPartition(FlightOrigin:org.apache.spark.sql.DataFrame,numPartitions:Int):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "createWeatherOriginPartition", "tail" : "(WeatherOrigin: DataFrame, numPartitions: Int): DataFrame", "member" : "com.join.DataJoin.createWeatherOriginPartition", "link" : "com\/join\/DataJoin.html#createWeatherOriginPartition(WeatherOrigin:org.apache.spark.sql.DataFrame,numPartitions:Int):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "createWeatherOriginDataset", "tail" : "(weather_ds: Dataset[Weather]): DataFrame", "member" : "com.join.DataJoin.createWeatherOriginDataset", "link" : "com\/join\/DataJoin.html#createWeatherOriginDataset(weather_ds:org.apache.spark.sql.Dataset[com.preprocess.weather.Weather]):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "createFlightsOriginDataset", "tail" : "(flight_ds: Dataset[Flight]): DataFrame", "member" : "com.join.DataJoin.createFlightsOriginDataset", "link" : "com\/join\/DataJoin.html#createFlightsOriginDataset(flight_ds:org.apache.spark.sql.Dataset[com.preprocess.flights.Flight]):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "getCurrentTime", "tail" : "(): Long", "member" : "com.join.DataJoin.getCurrentTime", "link" : "com\/join\/DataJoin.html#getCurrentTime:Long", "kind" : "def"}, {"member" : "com.join.DataJoin#<init>", "error" : "unsupported entity"}, {"label" : "synchronized", "tail" : "(arg0: ⇒ T0): T0", "member" : "scala.AnyRef.synchronized", "link" : "com\/join\/DataJoin.html#synchronized[T0](x$1:=>T0):T0", "kind" : "final def"}, {"label" : "##", "tail" : "(): Int", "member" : "scala.AnyRef.##", "link" : "com\/join\/DataJoin.html###():Int", "kind" : "final def"}, {"label" : "!=", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.!=", "link" : "com\/join\/DataJoin.html#!=(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "==", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.==", "link" : "com\/join\/DataJoin.html#==(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "ne", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.ne", "link" : "com\/join\/DataJoin.html#ne(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "eq", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.eq", "link" : "com\/join\/DataJoin.html#eq(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "finalize", "tail" : "(): Unit", "member" : "scala.AnyRef.finalize", "link" : "com\/join\/DataJoin.html#finalize():Unit", "kind" : "def"}, {"label" : "wait", "tail" : "(arg0: Long, arg1: Int): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/join\/DataJoin.html#wait(x$1:Long,x$2:Int):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(arg0: Long): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/join\/DataJoin.html#wait(x$1:Long):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/join\/DataJoin.html#wait():Unit", "kind" : "final def"}, {"label" : "notifyAll", "tail" : "(): Unit", "member" : "scala.AnyRef.notifyAll", "link" : "com\/join\/DataJoin.html#notifyAll():Unit", "kind" : "final def"}, {"label" : "notify", "tail" : "(): Unit", "member" : "scala.AnyRef.notify", "link" : "com\/join\/DataJoin.html#notify():Unit", "kind" : "final def"}, {"label" : "toString", "tail" : "(): String", "member" : "scala.AnyRef.toString", "link" : "com\/join\/DataJoin.html#toString():String", "kind" : "def"}, {"label" : "clone", "tail" : "(): AnyRef", "member" : "scala.AnyRef.clone", "link" : "com\/join\/DataJoin.html#clone():Object", "kind" : "def"}, {"label" : "equals", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.equals", "link" : "com\/join\/DataJoin.html#equals(x$1:Any):Boolean", "kind" : "def"}, {"label" : "hashCode", "tail" : "(): Int", "member" : "scala.AnyRef.hashCode", "link" : "com\/join\/DataJoin.html#hashCode():Int", "kind" : "def"}, {"label" : "getClass", "tail" : "(): Class[_]", "member" : "scala.AnyRef.getClass", "link" : "com\/join\/DataJoin.html#getClass():Class[_]", "kind" : "final def"}, {"label" : "asInstanceOf", "tail" : "(): T0", "member" : "scala.Any.asInstanceOf", "link" : "com\/join\/DataJoin.html#asInstanceOf[T0]:T0", "kind" : "final def"}, {"label" : "isInstanceOf", "tail" : "(): Boolean", "member" : "scala.Any.isInstanceOf", "link" : "com\/join\/DataJoin.html#isInstanceOf[T0]:Boolean", "kind" : "final def"}], "class" : "com\/join\/DataJoin.html", "kind" : "class"}, {"name" : "com.join.DataJoinUtils", "shortDescription" : "", "object" : "com\/join\/DataJoinUtils$.html", "members_object" : [{"label" : "rowToWeather", "tail" : "(row: Row): Weather", "member" : "com.join.DataJoinUtils.rowToWeather", "link" : "com\/join\/DataJoinUtils$.html#rowToWeather(row:org.apache.spark.sql.Row):com.preprocess.weather.Weather", "kind" : "def"}, {"label" : "synchronized", "tail" : "(arg0: ⇒ T0): T0", "member" : "scala.AnyRef.synchronized", "link" : "com\/join\/DataJoinUtils$.html#synchronized[T0](x$1:=>T0):T0", "kind" : "final def"}, {"label" : "##", "tail" : "(): Int", "member" : "scala.AnyRef.##", "link" : "com\/join\/DataJoinUtils$.html###():Int", "kind" : "final def"}, {"label" : "!=", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.!=", "link" : "com\/join\/DataJoinUtils$.html#!=(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "==", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.==", "link" : "com\/join\/DataJoinUtils$.html#==(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "ne", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.ne", "link" : "com\/join\/DataJoinUtils$.html#ne(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "eq", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.eq", "link" : "com\/join\/DataJoinUtils$.html#eq(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "finalize", "tail" : "(): Unit", "member" : "scala.AnyRef.finalize", "link" : "com\/join\/DataJoinUtils$.html#finalize():Unit", "kind" : "def"}, {"label" : "wait", "tail" : "(arg0: Long, arg1: Int): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/join\/DataJoinUtils$.html#wait(x$1:Long,x$2:Int):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(arg0: Long): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/join\/DataJoinUtils$.html#wait(x$1:Long):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/join\/DataJoinUtils$.html#wait():Unit", "kind" : "final def"}, {"label" : "notifyAll", "tail" : "(): Unit", "member" : "scala.AnyRef.notifyAll", "link" : "com\/join\/DataJoinUtils$.html#notifyAll():Unit", "kind" : "final def"}, {"label" : "notify", "tail" : "(): Unit", "member" : "scala.AnyRef.notify", "link" : "com\/join\/DataJoinUtils$.html#notify():Unit", "kind" : "final def"}, {"label" : "toString", "tail" : "(): String", "member" : "scala.AnyRef.toString", "link" : "com\/join\/DataJoinUtils$.html#toString():String", "kind" : "def"}, {"label" : "clone", "tail" : "(): AnyRef", "member" : "scala.AnyRef.clone", "link" : "com\/join\/DataJoinUtils$.html#clone():Object", "kind" : "def"}, {"label" : "equals", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.equals", "link" : "com\/join\/DataJoinUtils$.html#equals(x$1:Any):Boolean", "kind" : "def"}, {"label" : "hashCode", "tail" : "(): Int", "member" : "scala.AnyRef.hashCode", "link" : "com\/join\/DataJoinUtils$.html#hashCode():Int", "kind" : "def"}, {"label" : "getClass", "tail" : "(): Class[_]", "member" : "scala.AnyRef.getClass", "link" : "com\/join\/DataJoinUtils$.html#getClass():Class[_]", "kind" : "final def"}, {"label" : "asInstanceOf", "tail" : "(): T0", "member" : "scala.Any.asInstanceOf", "link" : "com\/join\/DataJoinUtils$.html#asInstanceOf[T0]:T0", "kind" : "final def"}, {"label" : "isInstanceOf", "tail" : "(): Boolean", "member" : "scala.Any.isInstanceOf", "link" : "com\/join\/DataJoinUtils$.html#isInstanceOf[T0]:Boolean", "kind" : "final def"}], "kind" : "object"}], "com.preprocess.flights" : [{"name" : "com.preprocess.flights.Flight", "shortDescription" : "", "members_case class" : [{"member" : "com.preprocess.flights.Flight#<init>", "error" : "unsupported entity"}, {"label" : "ARR_DELAY_NEW", "tail" : ": Double", "member" : "com.preprocess.flights.Flight.ARR_DELAY_NEW", "link" : "com\/preprocess\/flights\/Flight.html#ARR_DELAY_NEW:Double", "kind" : "val"}, {"label" : "SCHEDULED_ARRIVAL_TIMESTAMP", "tail" : ": Timestamp", "member" : "com.preprocess.flights.Flight.SCHEDULED_ARRIVAL_TIMESTAMP", "link" : "com\/preprocess\/flights\/Flight.html#SCHEDULED_ARRIVAL_TIMESTAMP:java.sql.Timestamp", "kind" : "val"}, {"label" : "CRS_DEP_TIMESTAMP", "tail" : ": Timestamp", "member" : "com.preprocess.flights.Flight.CRS_DEP_TIMESTAMP", "link" : "com\/preprocess\/flights\/Flight.html#CRS_DEP_TIMESTAMP:java.sql.Timestamp", "kind" : "val"}, {"label" : "DEST_AIRPORT_ID", "tail" : ": Int", "member" : "com.preprocess.flights.Flight.DEST_AIRPORT_ID", "link" : "com\/preprocess\/flights\/Flight.html#DEST_AIRPORT_ID:Int", "kind" : "val"}, {"label" : "ORIGIN_AIRPORT_ID", "tail" : ": Int", "member" : "com.preprocess.flights.Flight.ORIGIN_AIRPORT_ID", "link" : "com\/preprocess\/flights\/Flight.html#ORIGIN_AIRPORT_ID:Int", "kind" : "val"}, {"label" : "synchronized", "tail" : "(arg0: ⇒ T0): T0", "member" : "scala.AnyRef.synchronized", "link" : "com\/preprocess\/flights\/Flight.html#synchronized[T0](x$1:=>T0):T0", "kind" : "final def"}, {"label" : "##", "tail" : "(): Int", "member" : "scala.AnyRef.##", "link" : "com\/preprocess\/flights\/Flight.html###():Int", "kind" : "final def"}, {"label" : "!=", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.!=", "link" : "com\/preprocess\/flights\/Flight.html#!=(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "==", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.==", "link" : "com\/preprocess\/flights\/Flight.html#==(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "ne", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.ne", "link" : "com\/preprocess\/flights\/Flight.html#ne(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "eq", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.eq", "link" : "com\/preprocess\/flights\/Flight.html#eq(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "finalize", "tail" : "(): Unit", "member" : "scala.AnyRef.finalize", "link" : "com\/preprocess\/flights\/Flight.html#finalize():Unit", "kind" : "def"}, {"label" : "wait", "tail" : "(arg0: Long, arg1: Int): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/flights\/Flight.html#wait(x$1:Long,x$2:Int):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(arg0: Long): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/flights\/Flight.html#wait(x$1:Long):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/flights\/Flight.html#wait():Unit", "kind" : "final def"}, {"label" : "notifyAll", "tail" : "(): Unit", "member" : "scala.AnyRef.notifyAll", "link" : "com\/preprocess\/flights\/Flight.html#notifyAll():Unit", "kind" : "final def"}, {"label" : "notify", "tail" : "(): Unit", "member" : "scala.AnyRef.notify", "link" : "com\/preprocess\/flights\/Flight.html#notify():Unit", "kind" : "final def"}, {"label" : "clone", "tail" : "(): AnyRef", "member" : "scala.AnyRef.clone", "link" : "com\/preprocess\/flights\/Flight.html#clone():Object", "kind" : "def"}, {"label" : "getClass", "tail" : "(): Class[_]", "member" : "scala.AnyRef.getClass", "link" : "com\/preprocess\/flights\/Flight.html#getClass():Class[_]", "kind" : "final def"}, {"label" : "asInstanceOf", "tail" : "(): T0", "member" : "scala.Any.asInstanceOf", "link" : "com\/preprocess\/flights\/Flight.html#asInstanceOf[T0]:T0", "kind" : "final def"}, {"label" : "isInstanceOf", "tail" : "(): Boolean", "member" : "scala.Any.isInstanceOf", "link" : "com\/preprocess\/flights\/Flight.html#isInstanceOf[T0]:Boolean", "kind" : "final def"}], "case class" : "com\/preprocess\/flights\/Flight.html", "kind" : "case class"}, {"name" : "com.preprocess.flights.FlightPreprocessing", "shortDescription" : "This class processes the flight table by: cleaning NA values and keeping only columns of interest", "members_class" : [{"label" : "getAirportList", "tail" : "(): DataFrame", "member" : "com.preprocess.flights.FlightPreprocessing.getAirportList", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#getAirportList:org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "getProcessedFlightData", "tail" : "(): DataFrame", "member" : "com.preprocess.flights.FlightPreprocessing.getProcessedFlightData", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#getProcessedFlightData:org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "buildFlightTable", "tail" : "(): DataFrame", "member" : "com.preprocess.flights.FlightPreprocessing.buildFlightTable", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#buildFlightTable():org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "buildAirportList", "tail" : "(df: DataFrame): DataFrame", "member" : "com.preprocess.flights.FlightPreprocessing.buildAirportList", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#buildAirportList(df:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "processData", "tail" : "(cleanedData: DataFrame): DataFrame", "member" : "com.preprocess.flights.FlightPreprocessing.processData", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#processData(cleanedData:org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame", "kind" : "def"}, {"label" : "cleanData", "tail" : "(): DataFrame", "member" : "com.preprocess.flights.FlightPreprocessing.cleanData", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#cleanData():org.apache.spark.sql.DataFrame", "kind" : "def"}, {"member" : "com.preprocess.flights.FlightPreprocessing#<init>", "error" : "unsupported entity"}, {"label" : "synchronized", "tail" : "(arg0: ⇒ T0): T0", "member" : "scala.AnyRef.synchronized", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#synchronized[T0](x$1:=>T0):T0", "kind" : "final def"}, {"label" : "##", "tail" : "(): Int", "member" : "scala.AnyRef.##", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html###():Int", "kind" : "final def"}, {"label" : "!=", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.!=", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#!=(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "==", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.==", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#==(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "ne", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.ne", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#ne(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "eq", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.eq", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#eq(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "finalize", "tail" : "(): Unit", "member" : "scala.AnyRef.finalize", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#finalize():Unit", "kind" : "def"}, {"label" : "wait", "tail" : "(arg0: Long, arg1: Int): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#wait(x$1:Long,x$2:Int):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(arg0: Long): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#wait(x$1:Long):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#wait():Unit", "kind" : "final def"}, {"label" : "notifyAll", "tail" : "(): Unit", "member" : "scala.AnyRef.notifyAll", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#notifyAll():Unit", "kind" : "final def"}, {"label" : "notify", "tail" : "(): Unit", "member" : "scala.AnyRef.notify", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#notify():Unit", "kind" : "final def"}, {"label" : "toString", "tail" : "(): String", "member" : "scala.AnyRef.toString", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#toString():String", "kind" : "def"}, {"label" : "clone", "tail" : "(): AnyRef", "member" : "scala.AnyRef.clone", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#clone():Object", "kind" : "def"}, {"label" : "equals", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.equals", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#equals(x$1:Any):Boolean", "kind" : "def"}, {"label" : "hashCode", "tail" : "(): Int", "member" : "scala.AnyRef.hashCode", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#hashCode():Int", "kind" : "def"}, {"label" : "getClass", "tail" : "(): Class[_]", "member" : "scala.AnyRef.getClass", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#getClass():Class[_]", "kind" : "final def"}, {"label" : "asInstanceOf", "tail" : "(): T0", "member" : "scala.Any.asInstanceOf", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#asInstanceOf[T0]:T0", "kind" : "final def"}, {"label" : "isInstanceOf", "tail" : "(): Boolean", "member" : "scala.Any.isInstanceOf", "link" : "com\/preprocess\/flights\/FlightPreprocessing.html#isInstanceOf[T0]:Boolean", "kind" : "final def"}], "class" : "com\/preprocess\/flights\/FlightPreprocessing.html", "kind" : "class"}], "com" : [{"name" : "com.DataProcessingApplication", "shortDescription" : "", "object" : "com\/DataProcessingApplication$.html", "members_object" : [{"label" : "main", "tail" : "(args: Array[String]): Unit", "member" : "com.DataProcessingApplication.main", "link" : "com\/DataProcessingApplication$.html#main(args:Array[String]):Unit", "kind" : "def"}, {"label" : "synchronized", "tail" : "(arg0: ⇒ T0): T0", "member" : "scala.AnyRef.synchronized", "link" : "com\/DataProcessingApplication$.html#synchronized[T0](x$1:=>T0):T0", "kind" : "final def"}, {"label" : "##", "tail" : "(): Int", "member" : "scala.AnyRef.##", "link" : "com\/DataProcessingApplication$.html###():Int", "kind" : "final def"}, {"label" : "!=", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.!=", "link" : "com\/DataProcessingApplication$.html#!=(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "==", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.==", "link" : "com\/DataProcessingApplication$.html#==(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "ne", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.ne", "link" : "com\/DataProcessingApplication$.html#ne(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "eq", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.eq", "link" : "com\/DataProcessingApplication$.html#eq(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "finalize", "tail" : "(): Unit", "member" : "scala.AnyRef.finalize", "link" : "com\/DataProcessingApplication$.html#finalize():Unit", "kind" : "def"}, {"label" : "wait", "tail" : "(arg0: Long, arg1: Int): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/DataProcessingApplication$.html#wait(x$1:Long,x$2:Int):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(arg0: Long): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/DataProcessingApplication$.html#wait(x$1:Long):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/DataProcessingApplication$.html#wait():Unit", "kind" : "final def"}, {"label" : "notifyAll", "tail" : "(): Unit", "member" : "scala.AnyRef.notifyAll", "link" : "com\/DataProcessingApplication$.html#notifyAll():Unit", "kind" : "final def"}, {"label" : "notify", "tail" : "(): Unit", "member" : "scala.AnyRef.notify", "link" : "com\/DataProcessingApplication$.html#notify():Unit", "kind" : "final def"}, {"label" : "toString", "tail" : "(): String", "member" : "scala.AnyRef.toString", "link" : "com\/DataProcessingApplication$.html#toString():String", "kind" : "def"}, {"label" : "clone", "tail" : "(): AnyRef", "member" : "scala.AnyRef.clone", "link" : "com\/DataProcessingApplication$.html#clone():Object", "kind" : "def"}, {"label" : "equals", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.equals", "link" : "com\/DataProcessingApplication$.html#equals(x$1:Any):Boolean", "kind" : "def"}, {"label" : "hashCode", "tail" : "(): Int", "member" : "scala.AnyRef.hashCode", "link" : "com\/DataProcessingApplication$.html#hashCode():Int", "kind" : "def"}, {"label" : "getClass", "tail" : "(): Class[_]", "member" : "scala.AnyRef.getClass", "link" : "com\/DataProcessingApplication$.html#getClass():Class[_]", "kind" : "final def"}, {"label" : "asInstanceOf", "tail" : "(): T0", "member" : "scala.Any.asInstanceOf", "link" : "com\/DataProcessingApplication$.html#asInstanceOf[T0]:T0", "kind" : "final def"}, {"label" : "isInstanceOf", "tail" : "(): Boolean", "member" : "scala.Any.isInstanceOf", "link" : "com\/DataProcessingApplication$.html#isInstanceOf[T0]:Boolean", "kind" : "final def"}], "kind" : "object"}], "com.Utils" : [{"name" : "com.Utils.SparkSessionWrapper", "shortDescription" : "", "object" : "com\/Utils\/SparkSessionWrapper$.html", "members_object" : [{"label" : "spark", "tail" : ": SparkSession", "member" : "com.Utils.SparkSessionWrapper.spark", "link" : "com\/Utils\/SparkSessionWrapper$.html#spark:org.apache.spark.sql.SparkSession", "kind" : "lazy val"}, {"label" : "synchronized", "tail" : "(arg0: ⇒ T0): T0", "member" : "scala.AnyRef.synchronized", "link" : "com\/Utils\/SparkSessionWrapper$.html#synchronized[T0](x$1:=>T0):T0", "kind" : "final def"}, {"label" : "##", "tail" : "(): Int", "member" : "scala.AnyRef.##", "link" : "com\/Utils\/SparkSessionWrapper$.html###():Int", "kind" : "final def"}, {"label" : "!=", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.!=", "link" : "com\/Utils\/SparkSessionWrapper$.html#!=(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "==", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.==", "link" : "com\/Utils\/SparkSessionWrapper$.html#==(x$1:Any):Boolean", "kind" : "final def"}, {"label" : "ne", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.ne", "link" : "com\/Utils\/SparkSessionWrapper$.html#ne(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "eq", "tail" : "(arg0: AnyRef): Boolean", "member" : "scala.AnyRef.eq", "link" : "com\/Utils\/SparkSessionWrapper$.html#eq(x$1:AnyRef):Boolean", "kind" : "final def"}, {"label" : "finalize", "tail" : "(): Unit", "member" : "scala.AnyRef.finalize", "link" : "com\/Utils\/SparkSessionWrapper$.html#finalize():Unit", "kind" : "def"}, {"label" : "wait", "tail" : "(arg0: Long, arg1: Int): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/Utils\/SparkSessionWrapper$.html#wait(x$1:Long,x$2:Int):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(arg0: Long): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/Utils\/SparkSessionWrapper$.html#wait(x$1:Long):Unit", "kind" : "final def"}, {"label" : "wait", "tail" : "(): Unit", "member" : "scala.AnyRef.wait", "link" : "com\/Utils\/SparkSessionWrapper$.html#wait():Unit", "kind" : "final def"}, {"label" : "notifyAll", "tail" : "(): Unit", "member" : "scala.AnyRef.notifyAll", "link" : "com\/Utils\/SparkSessionWrapper$.html#notifyAll():Unit", "kind" : "final def"}, {"label" : "notify", "tail" : "(): Unit", "member" : "scala.AnyRef.notify", "link" : "com\/Utils\/SparkSessionWrapper$.html#notify():Unit", "kind" : "final def"}, {"label" : "toString", "tail" : "(): String", "member" : "scala.AnyRef.toString", "link" : "com\/Utils\/SparkSessionWrapper$.html#toString():String", "kind" : "def"}, {"label" : "clone", "tail" : "(): AnyRef", "member" : "scala.AnyRef.clone", "link" : "com\/Utils\/SparkSessionWrapper$.html#clone():Object", "kind" : "def"}, {"label" : "equals", "tail" : "(arg0: Any): Boolean", "member" : "scala.AnyRef.equals", "link" : "com\/Utils\/SparkSessionWrapper$.html#equals(x$1:Any):Boolean", "kind" : "def"}, {"label" : "hashCode", "tail" : "(): Int", "member" : "scala.AnyRef.hashCode", "link" : "com\/Utils\/SparkSessionWrapper$.html#hashCode():Int", "kind" : "def"}, {"label" : "getClass", "tail" : "(): Class[_]", "member" : "scala.AnyRef.getClass", "link" : "com\/Utils\/SparkSessionWrapper$.html#getClass():Class[_]", "kind" : "final def"}, {"label" : "asInstanceOf", "tail" : "(): T0", "member" : "scala.Any.asInstanceOf", "link" : "com\/Utils\/SparkSessionWrapper$.html#asInstanceOf[T0]:T0", "kind" : "final def"}, {"label" : "isInstanceOf", "tail" : "(): Boolean", "member" : "scala.Any.isInstanceOf", "link" : "com\/Utils\/SparkSessionWrapper$.html#isInstanceOf[T0]:Boolean", "kind" : "final def"}], "kind" : "object"}]};