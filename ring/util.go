package ring

func stringsCompare(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for _, s := range s1 {
		var flag bool = false
		for _, z := range s2 {
			if s == z {
				flag = true
				break
			}
		}
		// not found
		if !flag {
			return false
		}
	}
	return true
}
