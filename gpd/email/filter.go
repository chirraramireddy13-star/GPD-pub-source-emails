package email

// FilterAlreadyExistingEmails removes emails present in the Redshift lookup set.
func FilterAlreadyExistingEmails(emails []string, existingLookup map[string]struct{}) []string {
	if len(emails) == 0 {
		return []string{}
	}
	if len(existingLookup) == 0 {
		out := make([]string, len(emails))
		copy(out, emails)
		return out
	}

	newEmails := make([]string, 0, len(emails))
	for _, addr := range emails {
		if _, exists := existingLookup[addr]; exists {
			continue
		}
		newEmails = append(newEmails, addr)
	}

	return newEmails
}

func FilterOutKnownEmails(emails []string, known map[string]struct{}) []string {
	return FilterAlreadyExistingEmails(emails, known)
}
