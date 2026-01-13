import os

base_dir = r'C:\Users\M.S.Seshashayanan\.gemini\antigravity\scratch\CLI_Snowflake\semantic_sync'
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(root, file)
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check if future import already exists
            if 'from __future__ import annotations' not in content:
                # Add the import at the very beginning
                if content.startswith('"""'):
                    # If file starts with docstring, add after it
                    end_doc = content.find('"""', 3) + 3
                    new_content = content[:end_doc] + '\n\nfrom __future__ import annotations\n' + content[end_doc:]
                else:
                    new_content = 'from __future__ import annotations\n\n' + content
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                print(f'Updated: {filepath}')
            else:
                print(f'Already has import: {filepath}')

print('\nDone updating all files!')
