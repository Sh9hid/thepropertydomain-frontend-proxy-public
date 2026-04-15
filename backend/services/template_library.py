"""
Template Library — manages outreach templates, selection, and fill.

Owns the full lifecycle: seed defaults, CRUD, select best for channel+stage,
record engagement events, and fill placeholders for send.
"""
from __future__ import annotations

import logging
import random
import re
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from core.utils import now_iso
from models.sql_models import OutreachTemplate

logger = logging.getLogger(__name__)

# ─── Default templates (ADDENDUM A) ──────────────────────────────────────────

_EMAIL_SIGN_OFF = (
    "\n\nWarm regards,\n"
    "Nitin Puri\n"
    "Laing+Simmons Oakville | Windsor\n"
    "0430 042 041"
)

_DEFAULT_TEMPLATES: List[Dict[str, Any]] = [
    # ── SMS Templates ──────────────────────────────────────────────────
    {
        "name": "HOT-SMS-1",
        "channel": "sms",
        "stage": "hot",
        "trigger_match": "withdrawn|rescinded",
        "body": (
            "Hi {owner_first_name}, I noticed {address} was recently taken off market. "
            "If you're still considering your options, I'd love to share what similar homes "
            "nearby have sold for. \u2014 Nitin, L+S Oakville"
        ),
        "style": "standard",
    },
    {
        "name": "HOT-SMS-2",
        "channel": "sms",
        "stage": "hot",
        "trigger_match": "mortgage",
        "body": (
            "Hi {owner_first_name}, rates are shifting and a lot of owners in {suburb} "
            "are reviewing their position. Happy to run a quick market update for {address} "
            "if useful. \u2014 Nitin, L+S Oakville"
        ),
        "style": "standard",
    },
    {
        "name": "HOT-SMS-3",
        "channel": "sms",
        "stage": "hot",
        "body": (
            "{owner_first_name}, there's been strong buyer demand in {suburb} this month. "
            "Want me to send you a free appraisal comparison for {address}? Takes 2 min. "
            "\u2014 Nitin, L+S Oakville"
        ),
        "style": "standard",
    },
    {
        "name": "WARM-SMS-1",
        "channel": "sms",
        "stage": "warm",
        "body": (
            "Hi {owner_first_name}, {suburb} median just hit ${suburb_median}. "
            "Your place at {address} could be sitting above that. Want me to check? "
            "\u2014 Nitin, L+S Oakville"
        ),
        "style": "standard",
    },
    {
        "name": "WARM-SMS-2",
        "channel": "sms",
        "stage": "warm",
        "body": (
            "{owner_first_name}, noticed some strong sales on your street recently. "
            "Happy to send you a quick snapshot of what that means for {address}. "
            "\u2014 Nitin, L+S Oakville"
        ),
        "style": "standard",
    },
    {
        "name": "NURTURE-SMS-1",
        "channel": "sms",
        "stage": "nurture",
        "body": (
            "Hi {owner_first_name}, just a quick market pulse from {suburb} \u2014 "
            "median up {suburb_growth}% this quarter. Let me know if you'd ever like "
            "a chat about {address}. \u2014 Nitin, L+S Oakville"
        ),
        "style": "standard",
    },
    # ── Email Templates ────────────────────────────────────────────────
    {
        "name": "COLD-EMAIL-HANDWRITTEN",
        "channel": "email",
        "stage": "cold",
        "style": "handwritten",
        "subject": "A note about {address}",
        "body": (
            "Hi {owner_first_name},\n\n"
            "I hope this finds you well. I wanted to reach out personally because "
            "I've been working closely with buyers and sellers in {suburb} and "
            "couldn't help but notice your property at {address}.\n\n"
            "There's been a lot of activity in your area recently \u2014 several homes "
            "nearby have sold above expectations, and buyer demand for streets like "
            "yours is genuinely strong right now.\n\n"
            "I'm not writing to pressure you into anything. I simply thought you might "
            "appreciate knowing what your home could be worth in today's market. "
            "If you're ever curious, I'd be happy to put together a no-obligation "
            "appraisal \u2014 just a clear picture of where things stand.\n\n"
            "Feel free to reply to this email or give me a call anytime."
            + _EMAIL_SIGN_OFF
        ),
    },
    {
        "name": "COLD-EMAIL-DATA",
        "channel": "email",
        "stage": "cold",
        "style": "data_led",
        "subject": "{address} \u2014 your property snapshot",
        "body": (
            "Hi {owner_first_name},\n\n"
            "I put together a quick snapshot for {address} based on recent "
            "market data:\n\n"
            "\u2022 {suburb} median house price: ${suburb_median}\n"
            "\u2022 Quarterly growth: {suburb_growth}%\n"
            "\u2022 Average days on market: {suburb_dom} days\n"
            "\u2022 Properties sold nearby (90 days): {nearby_sales_count}\n\n"
            "Your property sits in a pocket that's been outperforming the wider "
            "area. If you'd like a detailed, personalised appraisal comparing "
            "your home to the most relevant recent sales, I'm happy to prepare "
            "one \u2014 no cost, no obligation.\n\n"
            "Just reply to this email or call me directly."
            + _EMAIL_SIGN_OFF
        ),
    },
    {
        "name": "COLD-EMAIL-STORY",
        "channel": "email",
        "stage": "cold",
        "style": "story_led",
        "subject": "What's happening on your street",
        "body": (
            "Hi {owner_first_name},\n\n"
            "I was showing a buyer through a home near yours last week and they "
            "made a comment that stuck with me: \"This is exactly the kind of "
            "street we've been looking for.\"\n\n"
            "It got me thinking about {address}. {suburb} has seen genuine "
            "momentum lately, and streets like yours are attracting the kind of "
            "buyers who are ready to move quickly.\n\n"
            "I don't know if selling is something you've thought about, but if "
            "it ever is, I'd love to have a chat. Even if it's just to give you "
            "a sense of what the market looks like from where you're sitting.\n\n"
            "No pressure at all \u2014 just a genuine offer."
            + _EMAIL_SIGN_OFF
        ),
    },
    {
        "name": "DOORKNOCK-EMAIL",
        "channel": "email",
        "stage": "doorknock",
        "style": "handwritten",
        "subject": "Great to meet you today",
        "body": (
            "Hi {owner_first_name},\n\n"
            "It was great to meet you today at {address}. Thanks for taking the "
            "time to chat \u2014 I really appreciate it.\n\n"
            "As I mentioned, I've been working extensively in {suburb} and I'm "
            "seeing strong demand from qualified buyers right now. If you'd like "
            "me to put together a market appraisal so you can see exactly where "
            "your property sits, I'm happy to do that at no cost.\n\n"
            "Otherwise, I'll keep you posted on any noteworthy sales in your area. "
            "Feel free to reach out anytime."
            + _EMAIL_SIGN_OFF
        ),
    },
    {
        "name": "DOORKNOCK-LETTER-CURSIVE",
        "channel": "email",
        "stage": "doorknock",
        "style": "handwritten",
        "subject": "A personal note from our visit — {address}",
        "body": (
            '<div style="font-family: \'Georgia\', \'Instrument Serif\', serif; font-size: 17px; line-height: 1.8; '
            'color: #1a1a1a; max-width: 600px; margin: 0 auto; padding: 40px 32px; '
            'background: #fffdf7; border: 1px solid #e8e2d6; border-radius: 4px;">'
            '<p style="margin: 0 0 20px 0;">Dear {owner_first_name},</p>'
            '<p style="margin: 0 0 20px 0;">I wanted to take a moment to write to you personally after our conversation '
            'at <strong>{address}</strong> today. It\'s not often I get the chance to meet the people behind the '
            'properties I work with — and I genuinely enjoyed it.</p>'
            '<p style="margin: 0 0 20px 0;">{suburb} is a place I know well. I\'ve been working with families here '
            'for years, and I\'ve seen how the area has grown — not just in value, but in character. Your home '
            'is part of that story.</p>'
            '<p style="margin: 0 0 8px 0;">I put together a quick snapshot for you:</p>'
            '<div style="background: #f8f5ef; border-left: 3px solid #c4a265; padding: 16px 20px; '
            'margin: 16px 0 24px 0; border-radius: 0 4px 4px 0; font-size: 15px;">'
            '<div style="margin-bottom: 6px;"><strong>Your property</strong>: {address}</div>'
            '<div style="margin-bottom: 6px;"><strong>{suburb} median</strong>: ${suburb_median}</div>'
            '<div style="margin-bottom: 6px;"><strong>Recent nearby sales</strong>: {nearby_sales_count} in the last 90 days</div>'
            '<div style="margin-bottom: 6px;"><strong>Area growth</strong>: {suburb_growth}% this quarter</div>'
            '<div><strong>Average days on market</strong>: {suburb_dom} days</div>'
            '</div>'
            '<p style="margin: 0 0 20px 0;">I\'m not writing to sell you anything. I simply believe every homeowner '
            'deserves to know where they stand — especially when the market is moving the way it is right now.</p>'
            '<p style="margin: 0 0 20px 0;">If you ever want a more detailed, no-obligation appraisal — or even '
            'just a chat over coffee about the area — I\'m here. No pressure, no expiry date on that offer.</p>'
            '<p style="margin: 0 0 6px 0; font-style: italic;">With warm regards,</p>'
            '<p style="margin: 0 0 4px 0;"><strong>Nitin Puri</strong></p>'
            '<p style="margin: 0; color: #666; font-size: 14px;">Laing+Simmons Oakville | Windsor<br/>'
            '0430 042 041<br/>'
            '<a href="mailto:oakville@lsre.com.au" style="color: #8b7355;">oakville@lsre.com.au</a></p>'
            '</div>'
        ),
    },
    {
        "name": "DOORKNOCK-GOOD-SEEING-YOU",
        "channel": "email",
        "stage": "doorknock",
        "style": "handwritten",
        "subject": "Good seeing you, {owner_first_name}",
        "body": (
            '<div style="font-family: \'Georgia\', serif; font-size: 17px; line-height: 1.85; '
            'color: #2c2c2c; max-width: 580px; margin: 0 auto; padding: 44px 36px; '
            'background: #fffdf7; border: 1px solid #e8e2d6;">'
            '<p style="margin: 0 0 22px 0;">{owner_first_name},</p>'
            '<p style="margin: 0 0 22px 0;">Really good seeing you at <strong>{address}</strong>. '
            'Thanks for opening the door and having a chat \u2014 most people don\u2019t, '
            'so I genuinely appreciate it.</p>'
            '<p style="margin: 0 0 22px 0;">{suburb} is a place I know well. I\u2019ve been '
            'working with families here for a while now, and I can tell you \u2014 your street '
            'has a really lovely feel to it.</p>'
            '<p style="margin: 0 0 22px 0;">I\u2019ve attached a quick property snapshot for '
            '{address} based on recent comparable sales in the area. Have a look when you get '
            'a chance \u2014 no obligation, just thought you\u2019d find it interesting.</p>'
            '<p style="margin: 0 0 22px 0;">If you ever want a more detailed chat \u2014 '
            'or just a coffee and a straight conversation about the area \u2014 I\u2019m always '
            'happy to. No pressure, no expiry.</p>'
            '<p style="margin: 0 0 8px 0; color: #888; font-size: 14px;">'
            'Talk soon,</p>'
            '<p style="margin: 0 0 4px 0;"><strong style="font-size: 17px;">Nitin</strong></p>'
            '<p style="margin: 0; color: #888; font-size: 13px;">Nitin Puri<br/>'
            'Laing+Simmons Oakville | Windsor<br/>'
            '<a href="tel:0430042041" style="color: #8b7355; text-decoration: none;">0430 042 041</a>'
            ' · <a href="mailto:oakville@lsre.com.au" style="color: #8b7355; text-decoration: none;">'
            'oakville@lsre.com.au</a></p>'
            '<p style="margin: 24px 0 0 0; font-size: 12px; color: #aaa; font-style: italic; '
            'line-height: 1.6;">P.S. The figures in the attached report are based on comparable '
            'sales in your area and may not reflect the precise value of your home. If you\u2019d '
            'like, I\u2019m happy to run a quiet, personalised analysis that gives you a more '
            'accurate picture \u2014 just for you, no strings attached.</p>'
            '</div>'
        ),
    },
    {
        "name": "DOORKNOCK-YOUR-HOME-YOUR-CALL",
        "channel": "email",
        "stage": "doorknock",
        "style": "handwritten",
        "subject": "{address} \u2014 a few things I noticed",
        "body": (
            '<div style="font-family: \'Georgia\', serif; font-size: 17px; line-height: 1.85; '
            'color: #2c2c2c; max-width: 580px; margin: 0 auto; padding: 44px 36px; '
            'background: #fffdf7; border: 1px solid #e8e2d6;">'
            '<p style="margin: 0 0 22px 0;">Hi {owner_first_name},</p>'
            '<p style="margin: 0 0 22px 0;">Thanks again for the quick chat at your place. '
            'I know it\u2019s not every day someone knocks on your door to talk property \u2014 '
            'so I wanted to follow up properly, not with a sales pitch, but with something '
            'actually useful.</p>'
            '<p style="margin: 0 0 12px 0;">Three things I noticed about your area that '
            'I think you should know:</p>'
            '<div style="background: #f9f6f0; padding: 20px 24px; margin: 0 0 24px 0; '
            'border-radius: 6px; font-size: 15px; color: #3a3a3a;">'
            '<div style="margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid #e8e2d6;">'
            '<strong style="color: #c4a265;">\u2460</strong> '
            '{nearby_sales_count} homes sold nearby in the last 3 months. That\u2019s '
            'significant movement for {suburb}.</div>'
            '<div style="margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid #e8e2d6;">'
            '<strong style="color: #c4a265;">\u2461</strong> '
            'The suburb median is now ${suburb_median} \u2014 up {suburb_growth}% this quarter. '
            'Your end of {suburb} tends to track above that.</div>'
            '<div>'
            '<strong style="color: #c4a265;">\u2462</strong> '
            'With {ownership_duration_years} years of ownership, you\u2019re sitting on '
            'serious equity. Properties like yours at {address} would likely come in around '
            '${estimated_value_low}\u2013${estimated_value_high}.</div>'
            '</div>'
            '<p style="margin: 0 0 22px 0;">Your home, your call, your timeline. '
            'I\u2019m not chasing a listing. I just reckon everyone deserves to know '
            'what they\u2019re sitting on.</p>'
            '<p style="margin: 0 0 22px 0;">If you want the detailed report for {address}, '
            'just reply \u201cyes\u201d and I\u2019ll send it through. Takes me 10 minutes to put together.</p>'
            '<p style="margin: 0 0 8px 0; color: #888; font-size: 14px;">'
            'Cheers,</p>'
            '<p style="margin: 0 0 4px 0;"><strong style="font-size: 17px;">Nitin</strong></p>'
            '<p style="margin: 0; color: #888; font-size: 13px;">Nitin Puri<br/>'
            'Laing+Simmons Oakville | Windsor<br/>'
            '<a href="tel:0430042041" style="color: #8b7355; text-decoration: none;">0430 042 041</a>'
            ' · <a href="mailto:oakville@lsre.com.au" style="color: #8b7355; text-decoration: none;">'
            'oakville@lsre.com.au</a></p>'
            '</div>'
        ),
    },
    {
        "name": "DOORKNOCK-HANDWRITTEN-NOTE",
        "channel": "email",
        "stage": "doorknock",
        "style": "handwritten",
        "subject": "Just wanted to say thanks \u2014 {owner_first_name}",
        "body": (
            '<div style="font-family: \'Georgia\', serif; font-size: 17px; line-height: 1.9; '
            'color: #2c2c2c; max-width: 560px; margin: 0 auto; padding: 48px 36px; '
            'background: #fffdf7; border: 1px solid #e8e2d6;">'
            '<p style="margin: 0 0 22px 0;">Hi {owner_first_name},</p>'
            '<p style="margin: 0 0 22px 0;">It was really nice meeting you today. '
            'I don\u2019t always get a proper chat when I\u2019m walking through a neighbourhood, '
            'so I genuinely appreciated you taking the time.</p>'
            '<p style="margin: 0 0 22px 0;">Your part of {suburb} has such a good feel to it \u2014 '
            'the kind of street where you can tell people actually look after their places. '
            'I noticed that the moment I turned the corner.</p>'
            '<p style="margin: 0 0 12px 0;">I had a quick look at the numbers when I got back:</p>'
            '<div style="background: #f9f6f0; border: 1px solid #e8e2d6; border-left: 3px solid #c4a265; '
            'padding: 18px 22px; margin: 0 0 24px 0; border-radius: 0 4px 4px 0; font-size: 15px; '
            'color: #3a3a3a;">'
            '<div style="margin-bottom: 8px;"><strong style="color: #c4a265;">Estimated range</strong>: '
            '${estimated_value_low} \u2013 ${estimated_value_high}</div>'
            '<div><strong style="color: #c4a265;">Ownership</strong>: '
            '~{ownership_duration_years} years</div>'
            '</div>'
            '<p style="margin: 0 0 22px 0;">If you\u2019re ever curious what the numbers look like for '
            '{address}, I\u2019m happy to put something together. No rush, no agenda.</p>'
            '<p style="margin: 0 0 22px 0;">Either way, it was good to meet you. '
            'Enjoy the rest of your week.</p>'
            '<p style="margin: 0 0 8px 0; color: #888; font-size: 14px;">'
            'Talk soon,</p>'
            '<p style="margin: 0 0 4px 0;"><strong style="font-size: 17px;">Nitin</strong></p>'
            '<p style="margin: 0; color: #888; font-size: 13px;">Nitin Puri<br/>'
            'Laing+Simmons Oakville | Windsor<br/>'
            '<a href="tel:0430042041" style="color: #8b7355; text-decoration: none;">0430 042 041</a>'
            ' \u00b7 <a href="mailto:oakville@lsre.com.au" style="color: #8b7355; text-decoration: none;">'
            'oakville@lsre.com.au</a></p>'
            '</div>'
        ),
    },
    {
        "name": "NURTURE-FIRST-TOUCH",
        "channel": "email",
        "stage": "nurture",
        "style": "handwritten",
        "subject": "{suburb} \u2014 a quick heads up from your local agent",
        "body": (
            '<div style="font-family: \'Georgia\', serif; font-size: 17px; line-height: 1.85; '
            'color: #2c2c2c; max-width: 580px; margin: 0 auto; padding: 44px 36px; '
            'background: #fffdf7; border: 1px solid #e8e2d6;">'
            '<p style="margin: 0 0 22px 0;">Hi {owner_first_name},</p>'
            '<p style="margin: 0 0 22px 0;">I\u2019m Nitin \u2014 I work across {suburb} and the surrounding '
            'pockets, mostly helping people figure out where they stand with their property. '
            'Not a cold call, not a flyer \u2014 just a local who watches the numbers closely.</p>'
            '<p style="margin: 0 0 12px 0;">Here\u2019s what\u2019s happening in your area right now:</p>'
            '<div style="background: #f9f6f0; border: 1px solid #e8e2d6; border-left: 3px solid #c4a265; '
            'padding: 18px 22px; margin: 0 0 24px 0; border-radius: 0 4px 4px 0; font-size: 15px; '
            'color: #3a3a3a;">'
            '<div style="margin-bottom: 8px;"><strong style="color: #c4a265;">{suburb} median</strong>: '
            '${suburb_median}</div>'
            '<div style="margin-bottom: 8px;"><strong style="color: #c4a265;">Growth</strong>: '
            '{suburb_growth}% this quarter</div>'
            '<div style="margin-bottom: 8px;"><strong style="color: #c4a265;">Nearby sales</strong>: '
            '{nearby_sales_count} in the last 90 days</div>'
            '<div><strong style="color: #c4a265;">Avg days on market</strong>: '
            '{suburb_dom} days</div>'
            '</div>'
            '<p style="margin: 0 0 22px 0;">I keep a small list of homeowners I send local market '
            'updates to \u2014 nothing spammy, just real data when something interesting happens '
            'in your area.</p>'
            '<p style="margin: 0 0 22px 0;">Reply <strong>\u201cyes\u201d</strong> if you\u2019d like to stay '
            'in the loop \u2014 or just ignore this and I won\u2019t bother you again.</p>'
            '<p style="margin: 0 0 8px 0; color: #888; font-size: 14px;">'
            'Cheers,</p>'
            '<p style="margin: 0 0 4px 0;"><strong style="font-size: 17px;">Nitin</strong></p>'
            '<p style="margin: 0; color: #888; font-size: 13px;">Nitin Puri<br/>'
            'Laing+Simmons Oakville | Windsor<br/>'
            '<a href="tel:0430042041" style="color: #8b7355; text-decoration: none;">0430 042 041</a>'
            ' \u00b7 <a href="mailto:oakville@lsre.com.au" style="color: #8b7355; text-decoration: none;">'
            'oakville@lsre.com.au</a></p>'
            '</div>'
        ),
    },
    {
        "name": "HOT-EMAIL-CMA",
        "channel": "email",
        "stage": "hot",
        "style": "standard",
        "subject": "{address} \u2014 updated market comparison",
        "body": (
            "Hi {owner_first_name},\n\n"
            "Following up on our conversation \u2014 I've attached an updated "
            "comparative market analysis for {address}.\n\n"
            "Key highlights:\n"
            "\u2022 Estimated range based on recent comparable sales\n"
            "\u2022 Current buyer demand indicators for {suburb}\n"
            "\u2022 How your property compares to recent sales on nearby streets\n\n"
            "I'd love to walk you through the numbers in person. Would you have "
            "15 minutes this week for a quick catch-up? I can come to you.\n\n"
            "Let me know what works."
            + _EMAIL_SIGN_OFF
        ),
    },
    {
        "name": "NURTURE-EMAIL-NEWSLETTER",
        "channel": "email",
        "stage": "nurture",
        "style": "newsletter",
        "subject": "{suburb} market update \u2014 {month} {year}",
        "body": (
            "Hi {owner_first_name},\n\n"
            "Here's your {suburb} market update for {month} {year}:\n\n"
            "\u2022 Median house price: ${suburb_median}\n"
            "\u2022 Quarterly change: {suburb_growth}%\n"
            "\u2022 Properties sold (last 90 days): {nearby_sales_count}\n"
            "\u2022 Average days on market: {suburb_dom}\n\n"
            "What this means for you:\n"
            "The {suburb} market continues to show solid fundamentals. If you've "
            "been thinking about your next move \u2014 whether that's selling, "
            "refinancing, or simply understanding your equity position \u2014 I'm "
            "here to help.\n\n"
            "Reply to this email or call me anytime for a confidential chat."
            + _EMAIL_SIGN_OFF
        ),
    },
]


# ─── Seed ─────────────────────────────────────────────────────────────────────


async def seed_default_templates(session: AsyncSession) -> dict:
    """Seed default templates from ADDENDUM A if the table is empty.

    Returns {"seeded": int} with the count of templates inserted.
    """
    count_result = await session.execute(
        select(func.count()).select_from(OutreachTemplate)
    )
    existing = count_result.scalar() or 0
    if existing > 0:
        logger.info("OutreachTemplate table has %d rows — skipping seed.", existing)
        return {"seeded": 0}

    now = now_iso()
    seeded = 0
    for tpl in _DEFAULT_TEMPLATES:
        row = OutreachTemplate(
            id=str(uuid.uuid4()),
            name=tpl["name"],
            channel=tpl["channel"],
            stage=tpl["stage"],
            trigger_match=tpl.get("trigger_match"),
            subject=tpl.get("subject"),
            body=tpl["body"],
            style=tpl.get("style", "standard"),
            variant="A",
            active=True,
            created_at=now,
            updated_at=now,
        )
        session.add(row)
        seeded += 1

    await session.commit()
    logger.info("Seeded %d default outreach templates.", seeded)
    return {"seeded": seeded}


# ─── CRUD ─────────────────────────────────────────────────────────────────────


async def list_templates(
    session: AsyncSession,
    *,
    channel: str = None,
    stage: str = None,
    active_only: bool = True,
) -> list[dict]:
    """List templates with optional filters."""
    q = select(OutreachTemplate).order_by(OutreachTemplate.name)
    if active_only:
        q = q.where(OutreachTemplate.active == True)  # noqa: E712
    if channel:
        q = q.where(OutreachTemplate.channel == channel)
    if stage:
        q = q.where(OutreachTemplate.stage == stage)

    result = await session.execute(q)
    rows = result.scalars().all()
    return [_template_to_dict(r) for r in rows]


async def get_template(session: AsyncSession, template_id: str) -> dict | None:
    """Get a single template by ID."""
    result = await session.execute(
        select(OutreachTemplate).where(OutreachTemplate.id == template_id)
    )
    row = result.scalars().first()
    return _template_to_dict(row) if row else None


async def create_template(session: AsyncSession, data: dict) -> dict:
    """Create a new outreach template."""
    now = now_iso()
    row = OutreachTemplate(
        id=str(uuid.uuid4()),
        name=data.get("name", "Untitled"),
        channel=data.get("channel", "email"),
        stage=data.get("stage", "cold"),
        trigger_match=data.get("trigger_match"),
        subject=data.get("subject"),
        body=data.get("body", ""),
        style=data.get("style", "standard"),
        variant=data.get("variant", "A"),
        active=data.get("active", True),
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return _template_to_dict(row)


async def update_template(
    session: AsyncSession, template_id: str, data: dict
) -> dict | None:
    """Update an existing template. Returns None if not found."""
    result = await session.execute(
        select(OutreachTemplate).where(OutreachTemplate.id == template_id)
    )
    row = result.scalars().first()
    if not row:
        return None

    allowed = {
        "name", "channel", "stage", "trigger_match", "subject", "body",
        "style", "variant", "active",
    }
    for key, value in data.items():
        if key in allowed and hasattr(row, key):
            setattr(row, key, value)
    row.updated_at = now_iso()

    await session.commit()
    await session.refresh(row)
    return _template_to_dict(row)


# ─── Selection ────────────────────────────────────────────────────────────────


async def select_best_template(
    session: AsyncSession,
    *,
    channel: str,
    stage: str,
    lead: dict = None,
) -> dict | None:
    """Select the best template for a channel + stage combination.

    Strategy:
      - If any matching template has booking_count > 0, pick the one with the
        highest booking_rate (booking_count / send_count).
      - If a lead is provided and has a trigger_type, prefer templates whose
        trigger_match regex matches.
      - Otherwise pick at random from active matches.
    """
    q = (
        select(OutreachTemplate)
        .where(
            OutreachTemplate.channel == channel,
            OutreachTemplate.stage == stage,
            OutreachTemplate.active == True,  # noqa: E712
        )
    )
    result = await session.execute(q)
    candidates = list(result.scalars().all())
    if not candidates:
        return None

    # Narrow by trigger_match if lead provides a trigger_type
    trigger = (lead or {}).get("trigger_type", "") or ""
    if trigger:
        trigger_matched = [
            c for c in candidates
            if c.trigger_match and re.search(c.trigger_match, trigger, re.IGNORECASE)
        ]
        if trigger_matched:
            candidates = trigger_matched

    # Performance-based selection
    with_bookings = [c for c in candidates if (c.booking_count or 0) > 0]
    if with_bookings:
        best = max(
            with_bookings,
            key=lambda c: (c.booking_count or 0) / max(c.send_count or 1, 1),
        )
        return _template_to_dict(best)

    return _template_to_dict(random.choice(candidates))


# ─── Event tracking ──────────────────────────────────────────────────────────


async def record_template_event(
    session: AsyncSession, template_id: str, event_type: str
) -> None:
    """Increment send_count / open_count / reply_count / booking_count.

    event_type must be one of: send, open, reply, booking.
    """
    result = await session.execute(
        select(OutreachTemplate).where(OutreachTemplate.id == template_id)
    )
    row = result.scalars().first()
    if not row:
        logger.warning("record_template_event: template %s not found", template_id)
        return

    field_map = {
        "send": "send_count",
        "open": "open_count",
        "reply": "reply_count",
        "booking": "booking_count",
    }
    field = field_map.get(event_type)
    if not field:
        logger.warning("record_template_event: unknown event_type '%s'", event_type)
        return

    setattr(row, field, (getattr(row, field) or 0) + 1)
    row.updated_at = now_iso()
    await session.commit()


# ─── Fill ─────────────────────────────────────────────────────────────────────


def fill_template(
    template_body: str, lead: dict, subject: str = None
) -> dict:
    """Fill {placeholders} with lead data.

    Returns {"body": filled_body, "subject": filled_subject}.
    Missing fields are replaced with empty string.
    """
    import datetime

    now = datetime.datetime.now()
    # Build a lookup combining lead data with date helpers
    lookup: Dict[str, str] = {
        "month": now.strftime("%B"),
        "year": str(now.year),
    }
    if isinstance(lead, dict):
        for k, v in lead.items():
            lookup[k] = str(v) if v is not None else ""

    def _replace(match: re.Match) -> str:
        key = match.group(1)
        return lookup.get(key, "")

    placeholder_re = re.compile(r"\{(\w+)\}")
    filled_body = placeholder_re.sub(_replace, template_body or "")
    filled_subject = placeholder_re.sub(_replace, subject or "")

    return {"body": filled_body, "subject": filled_subject}


# ─── Internal helpers ─────────────────────────────────────────────────────────


def _template_to_dict(row: OutreachTemplate) -> dict:
    """Convert an OutreachTemplate ORM row to a plain dict."""
    booking_rate = 0.0
    if (row.send_count or 0) > 0 and (row.booking_count or 0) > 0:
        booking_rate = round(row.booking_count / row.send_count, 4)

    return {
        "id": row.id,
        "name": row.name,
        "channel": row.channel,
        "stage": row.stage,
        "trigger_match": row.trigger_match,
        "subject": row.subject,
        "body": row.body,
        "style": row.style,
        "variant": row.variant,
        "send_count": row.send_count or 0,
        "open_count": row.open_count or 0,
        "reply_count": row.reply_count or 0,
        "booking_count": row.booking_count or 0,
        "booking_rate": booking_rate,
        "ai_generated": row.ai_generated,
        "active": row.active,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }
