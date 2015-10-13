package com.asiainfo.ocdp.stream.constant

/**
 * Created by tsingfu on 15/8/18.
 */
object EventConstant {

  val NOTNEEDCACHE = 0
  val NEEDCACHE = 1

  val RealtimeTransmission = 0

	val EVENT_CACHE_PREFIX_NAME = "eventCache:"
	val EVENTCACHE_FIELD_ROWEVENTID_PREFIX_KEY = "Row:eventId:"
	val EVENTCACHE_FIELD_TIME_PREFIX_KEY = "Time:"
	val EVENTCACHE_FIELD_TIMEBEID_PREFIX_KEY = "Time:beid:"
	val EVENTCACHE_FIELD_TIMEEVENTID_PREFIX_KEY = "Time:eventId:"


	// business sale default interval
  val DEFAULTINTERVAL = 7 * 24 * 60 * 60 * 1000L

  // diff source default delay time
  val DEFAULTDELAYTIME = 30 * 60 * 1000L

  // when a user be saled, save it to codis as this field
  val LOCKTIMEFIELD = "locktime"
}
