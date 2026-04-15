# ROI Calculator — Developer Yield Gap Analysis

Laing+Simmons Oakville | Windsor

---

## Purpose

Show homeowners the concrete dollar gap between selling to a residential buyer vs. a developer. This is the single most persuasive tool in the kit — it turns an abstract pitch into an undeniable number.

---

## Calculator Inputs (What You Ask the Homeowner)

| Input | How to Get It | Example |
|-------|--------------|---------|
| **Property address** | They tell you | 14 Nelson Rd, Box Hill NSW 2765 |
| **Land size (sqm)** | From your lead data or ask them | 725 sqm |
| **Current zoning** | From your spatial data (NSW Planning Portal) | R3 Medium Density |
| **FSR (Floor Space Ratio)** | From your spatial data | 0.7:1 |
| **Street frontage (m)** | From your data or Google Maps | 18m |
| **Number of street frontages** | Corner block = 2 | 1 |
| **Current residential estimate** | From your prospectus or Domain AVM | $1,150,000 |

---

## Calculator Formula (How You Calculate the Developer Yield)

### Step 1: Maximum Gross Floor Area (GFA)

```
GFA = Land Size x FSR
    = 725 sqm x 0.7
    = 507.5 sqm
```

### Step 2: Estimated Dwelling Yield

```
Dwellings = GFA / Average Dwelling Size
          = 507.5 / 120 sqm (typical townhouse)
          = 4 townhouses (round down)
```

### Step 3: Gross Development Value (GDV)

```
GDV = Dwellings x Average Sale Price per Dwelling
    = 4 x $850,000
    = $3,400,000
```

### Step 4: Developer's Maximum Land Bid

```
Developer Land Bid = GDV x Land-to-GDV Ratio
                   = $3,400,000 x 0.30 (standard 25-35%)
                   = $1,020,001
```

*Note: The land-to-GDV ratio varies by market. 25-35% is standard for Sydney infill. Adjust based on your local data.*

### Step 5: The Yield Gap

```
Residential Estimate:    $1,150,000
Developer Land Bid:      $1,020,000 (base)
Developer Premium Bid:   $1,350,000 (competitive / scarce sites)

Developer Premium Gap:   $200,000 (17% above residential)
```

*In practice, developers pay ABOVE the base calculation for scarce sites, corner blocks, or sites near approved DAs — because they're pricing in the certainty of approval, not just the raw yield.*

---

## How to Present the Numbers

### On a Call (60 seconds)

> "So here's what the numbers look like for your property. Based on the R3 zoning and 0.7 FSR, a developer can fit approximately 4 townhouses on your 725 square metres. At current sale prices, that's a gross development value of around $3.4 million. Using a standard land-to-value ratio, the developer's land bid comes in around $1 million to $1.35 million. Your residential estimate is $1.15 million. So the gap isn't huge at the base, but when you factor in site scarcity and the DA activity in your corridor, developers are likely to bid a premium. That's where the real value is."

### In the Prospectus (Page 3)

| Metric | Value |
|--------|-------|
| Land area | 725 sqm |
| Zoning / FSR | R3 / 0.7:1 |
| Max GFA | 507 sqm |
| Estimated yield | 4 townhouses |
| Gross Development Value | $3,400,000 |
| Developer base land bid | $1,020,000 |
| Developer premium bid (scarce site) | $1,200,000 - $1,350,000 |
| Residential estimate | $1,150,000 |
| **Estimated developer premium** | **$50,000 - $200,000** |

---

## Mortgage Cliff ROI Calculator

### Inputs

| Input | Example |
|-------|---------|
| Current loan balance | $650,000 |
| Current fixed rate | 2.49% |
| Revert variable rate | 6.24% |
| Remaining loan term | 22 years |
| Property value | $1,150,000 |
| Current LVR | 56.5% |

### Calculation

```
Current monthly repayment (2.49%):     $2,950
New monthly repayment (6.24%):         $4,380
Monthly increase:                       $1,430
Annual increase:                        $17,160

Best refinance rate available (5.49%): $3,980
Monthly saving vs. revert rate:         $400
Annual saving:                          $4,800
```

### Present It

> "Right now you're paying $2,950 a month. When your fixed rate expires, that jumps to $4,380 — that's an extra $1,430 every month. Over a year, that's $17,000 you weren't budgeting for. Through Ownit1st, the best rate we can get you right now is 5.49%, which brings your repayment to $3,980 — saving you $400 a month compared to just rolling onto the revert rate. That's $4,800 a year back in your pocket."

---

## Quick Reference: Suburb Yield Benchmarks

| Suburb | Typical Land Price/sqm (Residential) | Developer Land Price/sqm | Premium |
|--------|--------------------------------------|-------------------------|---------|
| Box Hill | $1,400 | $1,700 - $2,100 | 21-50% |
| North Kellyville | $1,500 | $1,800 - $2,200 | 20-47% |
| Marsden Park | $1,200 | $1,400 - $1,800 | 17-50% |
| Riverstone | $1,100 | $1,300 - $1,700 | 18-55% |
| The Ponds | $1,600 | $1,800 - $2,100 | 13-31% |
| Kellyville | $1,800 | $2,100 - $2,600 | 17-44% |
| Rouse Hill | $1,700 | $2,000 - $2,500 | 18-47% |

*These are indicative ranges based on recent off-market transactions. Update quarterly.*

---

## Implementation Notes

**To build this as a web tool:**
- Input form: address, land size, zoning, FSR (auto-populate from lead data where possible)
- Output: visual comparison chart (residential bar vs. developer bar) with dollar gap highlighted
- CTA: "Request your full 7-page prospectus"
- Captures email for lead intake

**To build this in the prospectus:**
- Page 3 of the Nano Banana Pro template
- Bar chart: residential vs. developer yield
- Table with all calculation steps shown (transparency builds trust)
