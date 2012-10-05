package util

func StringAdd(s []string, a string) []string {
	o := s
	found := false
	for _, existing := range s {
		if a == existing {
			found = true
			return s
		}
	}
	if found == false {
		o = append(o, a)
	}
	return o

}

func StringUnion(s []string, a []interface{}) []string {
	o := s
	for _, entry := range a {
		found := false
		for _, existing := range s {
			if entry.(string) == existing {
				found = true
				break
			}
		}
		if found == false {
			o = append(o, entry.(string))
		}
	}
	return o
}
