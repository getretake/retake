import Stripe from "stripe";
import { NextResponse } from "next/server";
import { withStripeCustomerId } from "@/utils/api";

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY ?? "");

const GET = withStripeCustomerId(async ({ id }) => {
  const subscriptions = await stripe.subscriptions.list({
    customer: id,
    status: "active",
  });

  return NextResponse.json({ subscriptions });
});

export { GET };
