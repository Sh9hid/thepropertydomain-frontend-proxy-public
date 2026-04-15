# Outreach Identity Rules

## Channel Identity

| Channel | Identity | Name | Phone | Email |
|---|---|---|---|---|
| Calls | Shahid | Shahid | 04 85 85 7881 | — |
| SMS | Shahid | Shahid | 04 85 85 7881 | — |
| Email (L+S) | Nitin Puri | Nitin Puri | 0430 042 041 | oakville@lsre.com.au |
| Email (Ownit1st) | Shahid | Shahid | 04 85 85 7881 | info@ownit1stloans.com.au |

## Rules

1. **Never** use "Ownit1st", "Hills Intelligence Hub", or "Shahid" in homeowner-facing L+S copy.
2. **Never** use "Nitin Puri" or "Laing+Simmons" in Ownit1st mortgage outreach.
3. SMS approval flow: operator approves message before it sends (not autonomous).
4. Email compose: pre-filled from `recommended_email` from the terminal endpoint.

## SMS Flow

1. Lead shows in CommandLedger with phone
2. Operator opens EntityOS → LeadWorkspace → SMS tab
3. Message pre-filled from `what_to_say` / `recommended_sms_message`
4. Operator reviews and approves
5. `POST /api/leads/{id}/send-sms` fires

## Email Flow

1. Operator opens EntityOS → click "Compose email" in header OR go to email tab
2. Subject + body pre-filled from `recommended_email.subject` / `recommended_email.body`
3. Operator edits, clicks Send
4. `POST /api/leads/{id}/send-email` fires via SMTP

## Brand Names

- L+S: `Laing+Simmons Oakville | Windsor`
- Mortgage: `Ownit1st Loans`
- Principal (L+S): `Nitin Puri` — `oakville@lsre.com.au` — `0430 042 041`
- Operator: `Shahid` — `04 85 85 7881` — `info@ownit1stloans.com.au`
