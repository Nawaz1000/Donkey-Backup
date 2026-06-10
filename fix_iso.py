import os
import re

for r, d, files in os.walk('backend'):
    for file in files:
        if file.endswith('.py') and not file.startswith('patch_'):
            path = os.path.join(r, file)
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Use negative lookahead to ensure we don't append "Z" if it's already there
            new_content = re.sub(r'datetime\.utcnow\(\)\.isoformat\(\)(?!\s*\+\s*"Z")', r'datetime.utcnow().isoformat() + "Z"', content)
            new_content = re.sub(r'now\.isoformat\(\)(?!\s*\+\s*"Z")', r'now.isoformat() + "Z"', new_content)
            
            if content != new_content:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                print(f"Updated {path}")
