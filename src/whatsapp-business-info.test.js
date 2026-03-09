import { describe, expect, it } from "vitest";
import {
  buildBusinessInfoAiContext,
  buildBusinessInfoReplyTemplate,
  detectBusinessInfoIntent,
  normalizeBusinessInfo,
} from "./whatsapp-business-info.js";

describe("normalizeBusinessInfo", () => {
  it("prefers explicit business identity fields", () => {
    expect(
      normalizeBusinessInfo({
        business_name: "Shree Marble",
        whatsapp_name: "Ignored WA Name",
        business_category: "Marble Shop",
        business_address: "Plot 12, Jaipur",
        business_hours: "10 AM to 7 PM",
        business_map_url: "https://maps.example/test",
        whatsapp_number: "919999999999",
        phone: "911111111111",
        email: "hello@example.com",
      })
    ).toEqual({
      brandName: "Shree Marble",
      category: "Marble Shop",
      address: "Plot 12, Jaipur",
      hours: "10 AM to 7 PM",
      mapUrl: "https://maps.example/test",
      phone: "919999999999",
      email: "hello@example.com",
    });
  });
});

describe("detectBusinessInfoIntent", () => {
  it("detects Hindi location questions", () => {
    expect(
      detectBusinessInfoIntent({
        normalizedText: "location janni hai",
        rawText: "Location janni hai",
      })
    ).toBe("location");
    expect(
      detectBusinessInfoIntent({
        normalizedText: "",
        rawText: "आपका पता क्या है",
      })
    ).toBe("location");
  });
});

describe("buildBusinessInfoReplyTemplate", () => {
  const businessInfo = {
    business_name: "Demo Marble House",
    business_address: "Plot 12, Stone Market, Jaipur",
    business_hours: "10 AM to 7 PM",
    business_map_url: "https://maps.example/demo",
    phone: "9876543210",
    email: "hello@example.com",
  };

  it("returns structured Hindi location replies with business voice", () => {
    expect(
      buildBusinessInfoReplyTemplate({
        intent: "location",
        businessInfo,
        languageCode: "hi",
      })
    ).toContain("हमारी लोकेशन");
  });

  it("returns structured Hinglish contact replies", () => {
    expect(
      buildBusinessInfoReplyTemplate({
        intent: "contact",
        businessInfo,
        languageCode: "hinglish",
      })
    ).toContain("Hamari Contact Details");
  });
});

describe("buildBusinessInfoAiContext", () => {
  it("marks missing values instead of leaving the model to guess", () => {
    expect(
      buildBusinessInfoAiContext({
        business_category: "Retail",
      })
    ).toContain("Address: not available");
  });
});
