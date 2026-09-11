package email

// DeduplicateEmails removes duplicates within a single batch while preserving input order.
func DeduplicateEmails(emails []string) []string {
	if len(emails) == 0 {
		return []string{}
	}

	seen := make(map[string]struct{}, len(emails))
	unique := make([]string, 0, len(emails))
	for _, email := range emails {
		if _, exists := seen[email]; exists {
			continue
		}
		seen[email] = struct{}{}
		unique = append(unique, email)
	}

	return unique
}
