import re

path = r'D:\woonona-lead-machine\frontend\src\operator\OperatorApp.tsx'
content = open(path, encoding='utf-8').read()

# 1. Add testNumber state
if 'const [testNumber, setTestNumber]' not in content:
    content = content.replace('const [textSending, setTextSending] = useState(false);', 
        'const [textSending, setTextSending] = useState(false);\n  const [testNumber, setTestNumber] = useState("");')

# 2. Add sendTest function properly
if 'const sendTest =' not in content:
    sendTest_func = """
  const sendTest = async () => {
    if (!selectedLead?.id || !textDraft.account_id) {
      setError("Select a lead and account first.");
      return;
    }
    const num = window.prompt("Enter test phone number:", testNumber || "");
    if (!num) return;
    setTestNumber(num);
    const msg = "SYSTEM TEST: Hills Intelligence Hub SMS integration is ACTIVE.";
    setTextSending(true);
    try {
      const result = await postJson<{ status: string }>(${API_BASE}/leads//send-text, {
        account_id: textDraft.account_id,
        recipient: num,
        message: msg,
        dry_run: false,
      });
      setTextFeedback(Test sent to : );
      await refreshAll(selectedDate, monthAnchor);
    } catch (err: any) {
      setError(err.message || "Test failed");
    } finally {
      setTextSending(false);
    }
  };

"""
    # Replace the mangled sendTest if it exists, otherwise add it.
    if 'const sendTest = async () => {' in content:
        # It's already there but potentially mangled
        start_idx = content.find('const sendTest = async () => {')
        end_idx = content.find('  const sendText = async (dryRun: boolean) => {')
        if start_idx != -1 and end_idx != -1:
            content = content[:start_idx] + sendTest_func + content[end_idx:]
    else:
        content = content.replace('const sendText = async (dryRun: boolean) => {', 
            sendTest_func + '  const sendText = async (dryRun: boolean) => {')

# 3. Clean up the button
# Make sure we don't have multiple Test SMS buttons
if 'Send Test SMS' not in content:
    content = content.replace('Test text', 'Send Test SMS')

# Final fix for the button onClick
content = re.sub(r'onClick={() => void sendText\(false\)} disabled={textSending}>\s*<Phone size={16} /> Send Test SMS', 
                 r'onClick={() => void sendTest()} disabled={textSending}><Phone size={16} /> Send Test SMS', content)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
