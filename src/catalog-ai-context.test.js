import { describe, expect, it } from "vitest";
import {
  buildCatalogAiContext,
  buildCatalogAvailabilityReply,
  buildCatalogGreetingPreview,
  buildCatalogListReply,
  buildCatalogPriceReply,
  collectCatalogComparableTerms,
  findCatalogItemByPrice,
  parseCatalogKeywords,
} from "./catalog-ai-context.js";

describe("parseCatalogKeywords", () => {
  it("parses keyword strings into clean entries", () => {
    expect(parseCatalogKeywords("granite, countertop; kitchen\nstone")).toEqual([
      "granite",
      "countertop",
      "kitchen",
      "stone",
    ]);
  });
});

describe("collectCatalogComparableTerms", () => {
  it("includes names, categories, and aliases for matching", () => {
    expect(
      collectCatalogComparableTerms({
        services: [
          {
            name: "Granite Polishing",
            category: "Stone Care",
            keywords: "granite polish, shine",
          },
        ],
        products: [],
      })
    ).toEqual(
      expect.arrayContaining([
        "granite polishing",
        "stone care",
        "granite polish",
        "shine",
      ])
    );
  });
});

describe("buildCatalogAiContext", () => {
  it("includes richer catalog knowledge for the AI prompt", () => {
    const context = buildCatalogAiContext({
      catalog: {
        services: [
          {
            name: "Granite Polishing",
            category: "Stone Care",
            description: "Restores shine on granite surfaces.",
            price_label: "250/sqft",
            duration_value: 2,
            duration_unit: "hours",
            details_prompt: "Ask for site size and current surface condition.",
            keywords: "granite polish, surface shine",
            is_bookable: true,
          },
        ],
        products: [
          {
            name: "Italian Marble Slab",
            category: "Marble",
            description: "Premium white marble slab.",
            price_label: "₹ 500 / sqft",
            quantity_value: 1,
            quantity_unit: "slab",
            details_prompt: "Ask for size, thickness, and finish.",
            keywords: ["marble slab", "italian stone"],
          },
        ],
      },
    });

    expect(context).toContain("Catalog summary: 1 active services and 1 active products are configured.");
    expect(context).toContain("ask for: Ask for site size and current surface condition.");
    expect(context).toContain("aliases: granite polish, surface shine");
    expect(context).toContain("Product: Italian Marble Slab");
  });
});

describe("buildCatalogGreetingPreview", () => {
  it("builds a greeting with real catalog offerings", () => {
    const preview = buildCatalogGreetingPreview({
      brandName: "Demo Marble House",
      catalog: {
        services: [
          {
            name: "Granite Polishing",
            price_label: "250/sqft",
            duration_value: 2,
            duration_unit: "hours",
          },
        ],
        products: [
          {
            name: "Italian Marble Slab",
            price_label: "₹ 500 / sqft",
            quantity_value: 1,
            quantity_unit: "slab",
          },
        ],
      },
    });

    expect(preview).toContain("Hi! Welcome to Demo Marble House.");
    expect(preview).toContain("*Quick Catalog Preview*");
    expect(preview).toContain("*Products*");
    expect(preview).toContain("1. *Italian Marble Slab*");
    expect(preview).toContain("*Services*");
    expect(preview).toContain("1. *Granite Polishing*");
  });
});

describe("findCatalogItemByPrice", () => {
  it("returns the cheapest priced item for the requested type", () => {
    const item = findCatalogItemByPrice({
      catalog: {
        services: [],
        products: [
          { name: "Starter Pack", price_label: "₹ 1,499" },
          { name: "Wellness Kit", price_label: "₹ 899" },
          { name: "Premium Pack", price_label: "₹ 2,999" },
        ],
      },
      itemType: "product",
      direction: "lowest",
    });

    expect(item?.name).toBe("Wellness Kit");
  });
});

describe("buildCatalogListReply", () => {
  it("builds a structured catalog list without menu-state wording", () => {
    const reply = buildCatalogListReply({
      brandName: "Rishab Khanna",
      itemType: "product",
      languageCode: "hinglish",
      catalog: {
        services: [],
        products: [
          { name: "Starter Pack", price_label: "₹ 1,499", quantity_value: 1, quantity_unit: "pack" },
          { name: "Wellness Kit", price_label: "₹ 899", quantity_value: 1, quantity_unit: "kit" },
        ],
      },
    });

    expect(reply).toContain("*Product Catalog*");
    expect(reply).toContain("Yeh products abhi available hain:");
    expect(reply).toContain("1. *Starter Pack*");
    expect(reply).toContain("2. *Wellness Kit*");
    expect(reply).toContain("Price: ₹ 1,499 | Pack: 1 pack");
    expect(reply).not.toContain("Reply with product number");
  });
});

describe("buildCatalogPriceReply", () => {
  it("builds a correct cheapest-item reply in Hinglish", () => {
    const reply = buildCatalogPriceReply({
      itemType: "product",
      direction: "lowest",
      languageCode: "hinglish",
      item: {
        name: "Wellness Kit",
        price_label: "₹ 899",
        quantity_value: 1,
        quantity_unit: "kit",
        description: "Affordable starter wellness bundle.",
      },
    });

    expect(reply).toContain("sabse sasta product *Wellness Kit*");
    expect(reply).toContain("*Price:* ₹ 899");
    expect(reply).toContain("*Pack:* 1 kit");
  });
});

describe("buildCatalogAvailabilityReply", () => {
  it("builds a matched service availability reply with real details", () => {
    const reply = buildCatalogAvailabilityReply({
      languageCode: "en",
      itemType: "service",
      matchedItem: {
        name: "Initial Consultation",
        category: "Consultation",
        description: "One-to-one consultation to understand your needs.",
        priceLabel: "₹ 499",
        durationLabel: "30 minutes",
        prompt: "Share your preferred date and time.",
      },
    });

    expect(reply).toContain("Yes, *Initial Consultation* is available.");
    expect(reply).toContain("*Initial Consultation*");
    expect(reply).toContain("*Duration:* 30 minutes");
    expect(reply).toContain("*Price:* ₹ 499");
    expect(reply).toContain("*Info Needed:* Share your preferred date and time.");
  });

  it("builds a polite unavailable reply with available alternatives", () => {
    const reply = buildCatalogAvailabilityReply({
      languageCode: "en",
      requestedName: "hair cut",
      itemType: "service",
      catalog: {
        services: [
          { name: "Initial Consultation" },
          { name: "Follow-up Visit" },
        ],
        products: [],
      },
    });

    expect(reply).toContain("*hair cut* is not available right now");
    expect(reply).toContain("*Available services right now*");
    expect(reply).toContain("1. *Initial Consultation*");
    expect(reply).toContain("2. *Follow-up Visit*");
  });
});
