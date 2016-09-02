package irms {

	object Params {

		// info of x
		object X {
			val xmin = 670 //included
			val xmax = 3702 //included
			val xstep = 4
			val vecsize = (xmax-xmin)/xstep + 1
			val xs = Range(X.xmin,X.xmax+X.xstep,X.xstep).map(1d*_) // x values
		}
		
	}

}
