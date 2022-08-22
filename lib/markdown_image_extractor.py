# fmt: off
import re

def getMarkdownImage(md: str) -> str | None:
	"""Gets the first image URL from a markdown string

	Args:
		md (str): Markdown string

	Returns:
		str | None: URL of the first image in the markdown string or None if no image found
	"""

	pattern = re.compile(r'!\[[^\]]*\]\((?P<filename>.*?)\s?(?=\"|\))(?P<optionalpart>\".*\")?\)')
	for match in pattern.finditer(md):
		match = match.groupdict()
		print(match)
		if 'filename' in match and re.match(r'^(https?:)?//', match['filename']):
			return match['filename']
	return None
