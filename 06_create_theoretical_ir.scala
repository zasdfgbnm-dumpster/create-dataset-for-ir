object CreateTheoreticalIR {
    
    def read(filename:String):Array[(Float,Float)] = {
        val source = Source.fromFile(filename)
        val lines = source.getLines
        var inside = false
        val header = "\"Frequency (cm-1)\"   \"Intensity (km/mol)\""
        val ending = "> <ROTATIONAL.CONSTANTS>"
        source.close()
    }
    
}